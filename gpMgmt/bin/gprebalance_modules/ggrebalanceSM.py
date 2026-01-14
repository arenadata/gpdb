#!/usr/bin/env python3

from transitions import Machine

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.commands.gp import GpMoveMirrors
    from gppylib.system.environment import *
    from gprebalance_modules.planner import *
    from gprebalance_modules.rebalance_schema import RebalanceSchema
    from gppylib.fault_injection import *
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class RebalanceSM:

    states = [
        'STATE_REBALANCE_START',
        'STATE_REBALANCE_MOVE_MIRRORS_START',
        'STATE_REBALANCE_MOVE_MIRRORS_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
        'STATE_REBALANCE_MOVE_PRIMARIES_START',
        'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
        'STATE_REBALANCE_END'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_REBALANCE_START',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_START'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_START',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_START',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_START'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_START',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_END',
            'source': ['STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE', 'STATE_REBALANCE_MOVE_MIRRORS_DONE'],
            'dest': 'STATE_REBALANCE_END'
        }
    ]


    def __init__(self, logger: Any, dburl: dbconn.DbURL, options: Any):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states,
                               transitions = self.transitions,
                               initial = 'STATE_REBALANCE_START',
                               before_state_change = 'on_every_state')

    def on_every_state(self) -> None:
        self.logger.info('REBALANCE - on_every_state')
        #if self.shutdown_requested:
        #    self.logger.info('Shrink was interrupted')
        #    raise Exception('Shrink was interrupted')
        #assert self.state in self.states + self.states_main_shrink_flow + self.states_rollback_flow
        #if self.state in self.states_main_shrink_flow + self.states_rollback_flow:
        #    self.rebalance_schema.storeState(self.state)

    def run(self, plan: Plan) -> None:
        self.rebalance_plan = plan
        if not self.rebalance_plan.getMoves():
            return

        # TODO: we actually change the order of moves.
        # what if it confuses the user?...
        self.moves_primaries = []
        self.moves_mirrors = []
        for move in self.rebalance_plan.getMoves():
            if move.seg.isSegmentPrimary() :
                self.moves_primaries.append(move)
            else:
                self.moves_mirrors.append(move)
        
        self.primary_segids_to_move = tuple([move.seg.getSegmentContentId() for move in self.moves_primaries])

        self.trigger('start')

    def process_moves(self, moves: List[LogicalMove]):
        filename = self.create_config_file(moves)
        gpmovemirrors_options = f' -i {filename}'

        if self.options.batch_size is not None:
            batch_size = self.options.batch_size
            # gpmovemirrors has its own limitation for batch size,
            # need to consider it here.
            if batch_size > MAX_COORDINATOR_NUM_WORKERS:
                batch_size = MAX_COORDINATOR_NUM_WORKERS
            gpmovemirrors_options += f' -B {batch_size}'

        cmd = GpMoveMirrors("Running gpmovemirrors", options=gpmovemirrors_options)
        cmd.run(validateAfter=True)
        # TODO: cleanup config files        

    def execute_role_swaps(self, segids: tuple[SegmentId]):
        """Execute multiple role swaps in single gprecoverseg -r call"""
        dbconn.execSQL(self.conn, "BEGIN")
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 't' WHERE "
                       f"content IN {segids} AND preferred_role = 'm'")
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'm' WHERE "
                       f"content IN {segids} AND preferred_role = 'p'")
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'p' WHERE "
                       f"content IN {segids} AND preferred_role = 't'")
        dbconn.execSQL(self.conn, "COMMIT")

        # TODO: refactor
        # TODO: specify log file location?...
        recoversegOptions = "-r -a"
        cmd = GpRecoverSeg("Running gprecoverseg", options=recoversegOptions)
        cmd.run(validateAfter=True)
    
    def create_config_file(self, moves: List[LogicalMove]) -> str:
        # TODO: do we really want to use /tmp location?
        filename = f'/tmp/ggrebalance_move_config_pid{os.getpid()}'
        with open(filename, 'w') as fp:
            for move in moves:
                segment_current_info = move.seg
                cfg_line = f'{segment_current_info.getSegmentHostName()}|{segment_current_info.getSegmentPort()}|{segment_current_info.getSegmentDataDirectory()} '
                cfg_line += f'{move.dstHost.hostname}|{move.target_port}|{move.target_datadir}\n'
                fp.write(cfg_line)
        return filename

    # state callbacks start here

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_START(self) -> None:
        self.logger.info('Rebalance - start moving mirrors')
        self.process_moves(self.moves_mirrors)
        self.logger.info('Rebalance - end moving mirrors')
        self.trigger('move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_DONE(self) -> None:
        if self.primary_segids_to_move:
            self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START')
        else:
            self.trigger('move_to_STATE_REBALANCE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_START(self) -> None:
        self.execute_role_swaps(self.primary_segids_to_move)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_START')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_START(self) -> None:
        self.logger.info('Rebalance - start moving primaries')
        self.process_moves(self.moves_primaries)
        self.logger.info('Rebalance - end moving primaries')
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_START(self) -> None:
        self.execute_role_swaps(self.primary_segids_to_move)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_END(self) -> None:
        self.conn.close()

    # state callbacks end here