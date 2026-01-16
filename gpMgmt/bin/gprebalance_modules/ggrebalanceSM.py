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
    from gprebalance_modules.shrink import GGShrink, DBNAME
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class GGRebalanceSM:

    states = [
        'STATE_START',
        'STATE_OPTIONS_VALIDATION',
        'STATE_CLEANUP',
        'STATE_ROLLBACK',
        'STATE_PLANNING_STARTED',
        'STATE_PLANNING_DONE',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_SETUP_SCHEMA_STARTED',
        'STATE_SETUP_SCHEMA_DONE',
        'STATE_EXECUTOR_STARTED',
        'STATE_EXECUTOR_DONE',
        'STATE_SHRINK_STARTED',
        'STATE_SHRINK_DONE',
        'STATE_REBALANCE_STARTED',
        'STATE_REBALANCE_DONE',
        'STATE_END',
        'STATE_ERROR'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_START',
            'dest': 'STATE_OPTIONS_VALIDATION'
        },
        {
            'trigger': 'move_to_STATE_CLEANUP',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_CLEANUP'
        },
        {
            'trigger': 'move_to_STATE_ROLLBACK',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_ROLLBACK'
        },
        {
            'trigger': 'move_to_STATE_PLANNING_STARTED',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_PLANNING_STARTED'
        },
        {
            'trigger': 'move_to_STATE_PLANNING_DONE',
            'source': 'STATE_PLANNING_STARTED',
            'dest': 'STATE_PLANNING_DONE'
        },
        {
            'trigger': 'move_to_STATE_CHECK_PREVIOUS_RUN',
            'source': 'STATE_PLANNING_DONE',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SCHEMA_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_SETUP_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SCHEMA_DONE',
            'source': 'STATE_SETUP_SCHEMA_STARTED',
            'dest': 'STATE_SETUP_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_EXECUTOR_STARTED',
            'source': ['STATE_SETUP_SCHEMA_DONE', 'STATE_CHECK_PREVIOUS_RUN'],
            'dest': 'STATE_EXECUTOR_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_STARTED',
            'source': 'STATE_EXECUTOR_STARTED',
            'dest': 'STATE_SHRINK_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_DONE',
            'source': 'STATE_SHRINK_STARTED',
            'dest': 'STATE_SHRINK_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_STARTED',
            'source': ['STATE_EXECUTOR_STARTED', 'STATE_SHRINK_DONE'],
            'dest': 'STATE_REBALANCE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': 'STATE_REBALANCE_STARTED',
            'dest': 'STATE_REBALANCE_DONE'
        },
        {
            'trigger': 'move_to_STATE_EXECUTOR_DONE',
            'source': ['STATE_EXECUTOR_STARTED', 'STATE_SHRINK_DONE', 'STATE_REBALANCE_DONE'],
            'dest': 'STATE_EXECUTOR_DONE'
        },
        {
            'trigger': 'move_to_STATE_END',
            'source': ['STATE_EXECUTOR_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_CLEANUP', 'STATE_ROLLBACK'],
            'dest': 'STATE_END'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    def __init__(self, logger: Any, dburl: dbconn.DbURL, options: Any, gpEnv: GpCoordinatorEnvironment, gpArray: gparray.GpArray, gpArrayDumpFilename: str):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.shutdown_requested = False
        self.gparray = gpArray
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)

        self.rebalance_schema = RebalanceSchema(self.conn)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

        self.gg_shrink = GGShrink(self.logger, self.dburl, self.options, gpEnv, self.gparray, gpArrayDumpFilename)
        self.gg_rebalance = RebalanceSM(self.logger, self.dburl, self.options)

        # Note: the plan for a shrink later will be provided by the planner component.
        # But for now we simply create a Plan object from the options directly.
        # We'll keep this stub till planner is ready.
        # If shrink_plan is None, we assume that we need to continue previous operation
        # TODO: remove the comment above?
        self.plan = None
        self.main_state_from_prev_run = self.rebalance_schema.getMainStateFromPreviousRun()


    def on_every_state(self) -> None:
        self.logger.info('MAIN - on_every_state')

        #if self.shutdown_requested:
        #    self.logger.info('Rebalance was interrupted')
        #    raise Exception('Rebalance was interrupted')

        self.rebalance_schema.storeMainState(self.state)

    def run(self) -> None:
        self.trigger('start')

    def shutdown(self) -> None:
        self.logger.info('GGRebalanceSM -  SHUTDOWN')
        need_exit = True
        #self.shutdown_requested = True
        if self.gg_shrink is not None:
            print('[RELOG] - gg_shrink.shutdown()')
            self.gg_shrink.shutdown()
            need_exit = False

        if self.gg_rebalance is not None:
            print('[RELOG] - gg_rebalance.shutdown()')
            self.gg_rebalance.shutdown()
            need_exit = False

        if need_exit:
            print('[RELOG] - sig_handler exit()')
            sys.exit(1)

    # state callbacks start here

    @wrap_state_func_with_faults
    def on_enter_STATE_OPTIONS_VALIDATION(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        elif self.options.rollback_required:
            self.trigger('move_to_STATE_ROLLBACK')
        else:
            self.trigger('move_to_STATE_PLANNING_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_CLEANUP(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        if not self.rebalance_schema.schemaExists():
            self.logger.info(f"Rebalance schema doesn't exist. Cleanup is not required.")
        else:
            self.logger.info(f">> self.main_state_from_prev_run  {self.main_state_from_prev_run}")
            # TODO: rework this ugly check
            self.gg_shrink.cleanup(self.main_state_from_prev_run == 'STATE_EXECUTOR_DONE' or self.main_state_from_prev_run == 'STATE_ROLLBACK')
            self.rebalance_schema.dropSchema()
            self.logger.info('Cleanup is complete')
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_ROLLBACK(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        self.gg_shrink.rollback()
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_PLANNING_STARTED(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        if self.options.target_segment_count != None:
            self.plan = Planner(self.logger, self.dburl, self.gparray, self.options).plan()

        if self.options.target_segment_count != None and self.options.show_plan:
            self.logger.info(f"Final plan:\n{self.plan}")
            #TODO: remove exit?
            sys.exit(0)

        self.trigger('move_to_STATE_PLANNING_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_PLANNING_DONE(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    @wrap_state_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        if not self.rebalance_schema.schemaExists():
            if self.plan == None:
                self.logger.error("Rebalance schema doesn't exists and no shrink plan is supplied. Please specify shrink plan.")
                self.trigger('move_to_STATE_ERROR')
                return
            if self.gparray.get_segment_count() < self.plan.target_segment_count:
                logger.error('Target segment count (%s) > current segment count (%s).\n'
                             'Currently only shrink is supported (target segment count < current segment count).'
                              % (self.plan.target_segment_count, self.gparray.get_segment_count()))
                self.trigger('move_to_STATE_ERROR')
                return
            self.trigger('move_to_STATE_SETUP_SCHEMA_STARTED')
        else:
            # Schema already exists from the previous run.
            # In this case we already have a plan saved in the schema,
            # and we'll continue (or rollback) according to it.
            # Or, if everything is complete, just exit.
            self.logger.info(f'main_state_from_prev_run {self.main_state_from_prev_run}')
            if self.main_state_from_prev_run == 'STATE_EXECUTOR_DONE':
                self.logger.info('Previous run was completed successfully. Please execute cleanup before a new run.')
                return

            if self.plan != None:
                self.logger.error("Can't start a new operation, because the previous one was interrupted. "
                                  "Please try to launch again without a plan to continue from the interrupted state, "
                                  "or use '--rollback' or '--cleanup' options.")
                self.trigger('move_to_STATE_ERROR')
                return

            self.trigger('move_to_STATE_EXECUTOR_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_STARTED(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        # Create schema and status tables.
        # It will also save plan in order to use it for recovering after interruption
        self.rebalance_schema.createSchema(self.plan)

        self.trigger('move_to_STATE_SETUP_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_DONE(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        self.logger.info(f'Created "{self.rebalance_schema.getSchemaName()}" schema')

        self.trigger('move_to_STATE_EXECUTOR_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_EXECUTOR_STARTED(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        shrink_state_from_prev_run = self.rebalance_schema.getShrinkStateFromPreviousRun()
        if not self.gg_shrink.state_is_final(shrink_state_from_prev_run):
            self.trigger('move_to_STATE_SHRINK_STARTED')
        else:
            self.trigger('move_to_STATE_REBALANCE_STARTED')
            #if not self.options.skip_rebalance:
            #    rebalance_state_from_prev_run = self.rebalance_schema.getRebalanceStateFromPreviousRun()
            #    if not self.gg_rebalance.state_is_final(rebalance_state_from_prev_run):
            #        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_EXECUTOR_DONE(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_STARTED(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        if self.plan is None or isinstance(self.plan, ShrinkPlan):
            self.gg_shrink.run(self.plan)

        self.trigger('move_to_STATE_SHRINK_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_DONE(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')

        if self.plan is not None and self.plan.getMoves() is not None:
            # what if plan is None? for ex., if we recovered after interruption during shrink?...
            self.gg_rebalance.run(self.plan)
            self.logger.info('Rebalance is complete')

        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        self.trigger('move_to_STATE_EXECUTOR_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_END(self) -> None:
        self.logger.info(f'MAIN STATE: {self.state}')
        #TODO: get rid of other connections?...
        self.conn.close()

    @wrap_state_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Main SM entered STATE_ERROR')

    # state callbacks end here

#################################
#################################

class RebalanceSM:

    states = [
        'STATE_REBALANCE_START',
        'STATE_REBALANCE_MOVE_MIRRORS_STARTED',
        'STATE_REBALANCE_MOVE_MIRRORS_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
        'STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
        'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
        'STATE_REBALANCE_DONE'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_REBALANCE_START',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_STARTED',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': ['STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE', 'STATE_REBALANCE_MOVE_MIRRORS_DONE'],
            'dest': 'STATE_REBALANCE_DONE'
        }
    ]


    def __init__(self, logger: Any, dburl: dbconn.DbURL, options: Any):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.shutdown_requested = False
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)

        self.rebalance_schema = RebalanceSchema(self.conn)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states,
                               transitions = self.transitions,
                               initial = 'STATE_REBALANCE_START',
                               before_state_change = 'on_every_state')

    def on_every_state(self) -> None:
        self.logger.info('REBALANCE - on_every_state')

        if self.shutdown_requested:
            self.logger.info('Rebalance was interrupted')
            raise Exception('Rebalance was interrupted')

        self.rebalance_schema.storeRebalanceState(self.state)

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
        
        self.primary_segids_to_move = [move.seg.getSegmentContentId() for move in self.moves_primaries]

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

        try:
            cmd = GpMoveMirrors("Running gpmovemirrors", options=gpmovemirrors_options)
            cmd.run(validateAfter=True)
        except Exception as e:
            error_msg = f"Error in gpmovemirrors process: {str(e)}"
            self.logger.error(error_msg)

        # TODO: cleanup config files        

    def execute_role_swaps(self, segids: List[SegmentId]):
        """Execute multiple role swaps in single gprecoverseg -r call"""
        dbconn.execSQL(self.conn, "BEGIN")
        seg_list = ', '.join(str(seg) for seg in segids)
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 't' WHERE "
                       f"content IN ({seg_list}) AND preferred_role = 'm'")
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'm' WHERE "
                       f"content IN ({seg_list}) AND preferred_role = 'p'")
        dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'p' WHERE "
                       f"content IN ({seg_list}) AND preferred_role = 't'")
        dbconn.execSQL(self.conn, "COMMIT")

        # TODO: refactor
        # TODO: specify log file location?...
        recoversegOptions = "-r -a"
        try:
            cmd = GpRecoverSeg("Running gprecoverseg", options=recoversegOptions)
            cmd.run(validateAfter=True)
        except Exception as e:
            error_msg = f"Error in gprecoverseg process: {str(e)}"
            self.logger.error(error_msg)

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

    def shutdown(self) -> None:
        self.logger.info('Rebalance - shutdown_requested set to True')
        self.shutdown_requested = True

    def state_is_final(self, state: str) -> bool:
        return state == self.states[-1]

    # state callbacks start here

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED(self) -> None:
        self.logger.info('Rebalance - start moving mirrors')
        self.process_moves(self.moves_mirrors)
        self.logger.info('Rebalance - end moving mirrors')
        self.trigger('move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_DONE(self) -> None:
        if self.primary_segids_to_move:
            self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED')
        else:
            self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED(self) -> None:
        self.execute_role_swaps(self.primary_segids_to_move)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_STARTED(self) -> None:
        self.logger.info('Rebalance - start moving primaries')
        self.process_moves(self.moves_primaries)
        self.logger.info('Rebalance - end moving primaries')
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED(self) -> None:
        self.execute_role_swaps(self.primary_segids_to_move)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        self.conn.close()

    # state callbacks end here