#!/usr/bin/env python3

from transitions import Machine
from enum import Enum

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.commands.gp import GpMoveMirrors
    from gppylib.system.environment import *
    from gprebalance_modules.planner import *
    from gprebalance_modules.rebalance_schema import RebalanceSchema, STATE_NOT_DEFINED
    from gprebalance_modules.rebalance_step import *
    from gppylib.fault_injection import *
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class RebalanceSM:

    states_not_logged = [
        'STATE_REBALANCE_INIT',
        'STATE_CHECK_PREVIOUS_RUN'
    ]

    states_main_rebalance_flow = [
        'STATE_REBALANCE_STARTED',
        'STATE_REBALANCE_PREPARE_MOVES_STARTED',
        'STATE_REBALANCE_PREPARE_MOVES_DONE',
        'STATE_REBALANCE_EXECUTION_STARTED',
        'STATE_REBALANCE_MOVES_SUCCEEDED',
        'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
        'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE',
        'STATE_REBALANCE_EXECUTION_DONE',
        'STATE_REBALANCE_DONE'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_REBALANCE_INIT',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_STARTED',
            'source': ['STATE_CHECK_PREVIOUS_RUN', 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'],
            'dest': 'STATE_REBALANCE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_PREPARE_MOVES_STARTED',
            'source': 'STATE_REBALANCE_STARTED',
            'dest': 'STATE_REBALANCE_PREPARE_MOVES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_PREPARE_MOVES_DONE',
            'source': 'STATE_REBALANCE_PREPARE_MOVES_STARTED',
            'dest': 'STATE_REBALANCE_PREPARE_MOVES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_STARTED',
            'source': ['STATE_REBALANCE_PREPARE_MOVES_DONE', 'STATE_REBALANCE_MOVES_SUCCEEDED', 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'],
            'dest': 'STATE_REBALANCE_EXECUTION_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVES_SUCCEEDED',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_MOVES_SUCCEEDED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_DONE',
            'dest': 'STATE_REBALANCE_DONE'
        }
    ]

    class RoleSwapDirection(Enum):
        PRIMARY_TO_MIRROR = 1
        MIRROR_TO_PRIMARY = 2

    def __init__(self, conn: dbconn.Connection, schema: RebalanceSchema, logger: Any, options: Any, gpArray: gparray.GpArray):
        self.logger = logger
        self.options = options
        self.shutdown_requested = False
        self.gparray = gpArray
        self.conn = conn
        self.rebalance_schema = schema
        self.cmd = None

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states_main_rebalance_flow + self.states_not_logged,
                               transitions = self.transitions,
                               initial = 'STATE_REBALANCE_INIT',
                               before_state_change = 'on_every_state')

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Rebalance was interrupted')
            raise Exception('Rebalance was interrupted')

        if self.state in self.states_main_rebalance_flow:
            self.rebalance_schema.storeRebalanceState(self.state)

    def run(self, plan: Plan) -> None:
        self.rebalance_plan = plan
        if not self.rebalance_plan.getMoves():
            return

        self.moves_primaries = []
        self.moves_mirrors = []
        for move in self.rebalance_plan.getMoves():
            if move.seg.isSegmentPrimary() :
                self.moves_primaries.append(move)
            else:
                self.moves_mirrors.append(move)
        
        self.primary_segments_to_move = [move.seg for move in self.moves_primaries]

        self.trigger('start')

    def process_moves(self, moves: List[LogicalMove]):
        if len(moves) == 0:
            return

        filename = self.create_config_file(moves)
        gpmovemirrors_options = f'-a -i {filename}'

        if self.options.batch_size is not None:
            batch_size = self.options.batch_size
            # gpmovemirrors has its own limitation for batch size,
            # need to consider it here.
            if batch_size > MAX_COORDINATOR_NUM_WORKERS:
                batch_size = MAX_COORDINATOR_NUM_WORKERS
            gpmovemirrors_options += f' -B {batch_size}'

        try:
            self.cmd = GpMoveMirrors("Running gpmovemirrors", options=gpmovemirrors_options)
            self.cmd.run(validateAfter=True)
        except Exception as e:
            logger.error(str(e))
            error_msg = f"Failed to execute 'gpmovemirrors {gpmovemirrors_options}'"
            raise Exception(error_msg)
        finally:
            self.cmd = None

        if os.path.exists(filename):
            os.remove(filename)

    def execute_role_swaps(self, segments_to_move: List[Segment], direction: RoleSwapDirection):
        """Execute multiple role swaps in single gprecoverseg -r call"""

        assert (len(segments_to_move) > 0)

        segids = [segment.getSegmentContentId() for segment in segments_to_move]
        dbids = [segment.getSegmentDbId() for segment in segments_to_move]

        seg_list = ', '.join(str(seg) for seg in segids)
        dbid_list = ', '.join(str(dbid) for dbid in dbids)

        dbconn.execSQL(self.conn, "BEGIN")

        # check the current status of 'preferred_role' and 'role' for all requested dbids
        # in order to recover properly from the previous interrupted run (if any)

        cnt_preferred_role_p = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE preferred_role = 'p' AND dbid IN ({dbid_list})")[0])
        cnt_role_p = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE role = 'p' AND dbid IN ({dbid_list})")[0])
        cnt_preferred_role_m = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE preferred_role = 'm' AND dbid IN ({dbid_list})")[0])
        cnt_role_m = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE role = 'm' AND dbid IN ({dbid_list})")[0])

        # if some have 'preferred_role'='p' and some have 'preferred_role'='m' - shouldn't happen, error out, needs to be resolved manually.
        # also some sanity check that there are no other values in catalog except 'm' and 'p' for 'preferred_role'.
        if ((cnt_preferred_role_p > 0 and cnt_preferred_role_m > 0) or
             (cnt_preferred_role_p + cnt_preferred_role_m != len(segids))):
            raise Exception("Error in catalog configuration: "
                            f"for dbid list ({dbid_list}) "
                            f"{cnt_preferred_role_p} have 'p' preferred role, and "
                            f"{cnt_preferred_role_m} have 'm' preferred role")

        is_catalog_update_required = False
        is_gprecoverseg_required = False

        if direction == self.RoleSwapDirection.PRIMARY_TO_MIRROR:
            # if all have 'preferred_role'='p' - it is our first run, need to update catalog and launch gprecoverseg
            if cnt_preferred_role_p == len(segids):
                is_catalog_update_required = True
                is_gprecoverseg_required = True
            else:
                # if all have 'preferred_role'='m' and not all have 'role'='m' - previous gprecoverseg was interrupted, need to launch it again
                if cnt_role_m != cnt_preferred_role_m:
                    is_gprecoverseg_required = True
                # if all have 'preferred_role'='m' and 'role'='m' - we've done everything on previous interrupted run, nothing to do
        else:
            # moving back in MIRROR_TO_PRIMARY direction
            # if all have 'preferred_role'='m' - it is our first run, need to update catalog and launch gprecoverseg
            if cnt_preferred_role_m == len(segids):
                is_catalog_update_required = True
                is_gprecoverseg_required = True
            else:
                # if all have 'preferred_role'='p' and not all have 'role'='p' - previous gprecoverseg was interrupted, need to launch it again
                if cnt_role_p != cnt_preferred_role_p:
                    is_gprecoverseg_required = True
                # if all have 'preferred_role'='p' and 'role'='p' - we've done everything on previous interrupted run, nothing to do

        if is_catalog_update_required:
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 't' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 'm'")
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'm' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 'p'")
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'p' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 't'")

        dbconn.execSQL(self.conn, "COMMIT")

        if direction == self.RoleSwapDirection.PRIMARY_TO_MIRROR:
            inject_fault('FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR')
        else:
            inject_fault('FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY')

        if is_gprecoverseg_required:
            recoverseg_options = "-r -a"
            try:
                self.cmd = GpRecoverSeg("Running gprecoverseg", options=recoverseg_options)
                self.cmd.run(validateAfter=True)
            except Exception as e:
                logger.error(str(e))
                error_msg = f"Failed to execute 'gprecoverseg {recoverseg_options}'"
                raise Exception(error_msg)
            finally:
                self.cmd = None

    def lookup_seg(self, seg: Segment) -> bool:
        """ Look up the segment gpdb by address, port, and dataDirectory """
        for db in self.gparray.getDbList():
            if (seg.getSegmentHostName() == db.getSegmentHostName() and
                seg.getSegmentPort() == db.getSegmentPort() and
                seg.getSegmentDataDirectory() == db.getSegmentDataDirectory()):
                return True
        return False

    def create_config_file(self, moves: List[LogicalMove]) -> str:
        filename = f'/tmp/ggrebalance_move_config_pid{os.getpid()}'
        with open(filename, 'w') as fp:
            for move in moves:
                segment_current_info = move.seg
                if not self.lookup_seg(segment_current_info):
                    self.logger.info(f'Skip segment for gpmovemirrors: {str(segment_current_info)}')
                    continue
                cfg_line = f'{segment_current_info.getSegmentHostName()}|{segment_current_info.getSegmentPort()}|{segment_current_info.getSegmentDataDirectory()} '
                cfg_line += f'{move.dstHost.hostname}|{move.target_port}|{move.target_datadir}\n'
                fp.write(cfg_line)
        return filename

    def shutdown(self) -> None:
        self.shutdown_requested = True
        if self.cmd != None:
            self.cmd.cancel()

    def state_is_final(self, state: str) -> bool:
        return state == self.states_main_rebalance_flow[-1]

    def get_state_after_interrupt(self, prev_state) -> str:
        if prev_state == 'STATE_REBALANCE_EXECUTION_STARTED' or \
           prev_state == 'STATE_REBALANCE_EXECUTION_SUCCEEDED' or \
           prev_state == 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE':
            return 'STATE_REBALANCE_EXECUTION_STARTED'

        prev_idx = self.states_main_rebalance_flow.index(prev_state)

        return self.states_main_rebalance_flow[prev_idx + 1]

    # state callbacks start here

    @wrap_state_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        state_from_prev_run = self.rebalance_schema.getRebalanceStateFromPreviousRun()

        if state_from_prev_run == STATE_NOT_DEFINED:
            self.trigger('move_to_STATE_REBALANCE_STARTED')
        elif self.state_is_final(state_from_prev_run):
            self.logger.info('Cluster is already rebalanced...')
        else:
            self.logger.info('Continue interrupted rebalance operation...')
            self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
            try:
                next_state = self.get_state_after_interrupt(state_from_prev_run)
            except:
                self.logger.error("Can't determine next state. Try to execute cleanup.")
                self.trigger('move_to_STATE_ERROR')
                return
            # use auto to_«state» method to recover
            self.trigger(f'to_{next_state}')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_PREPARE_MOVES_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_PREPARE_MOVES_STARTED(self) -> None:
        if not self.rebalance_plan.getMoves():
            raise Exception('Rebalance executor was launched with a plan without segment movements')

        rebalance_steps = []
        id = 0
        for move in self.rebalance_plan.getMoves():
            if move.seg.isSegmentPrimary():
                rebalance_steps.append(RebalanceStepSwitchoverToMirror(id, move))
                id += 1
                rebalance_steps.append(RebalanceStepMoveMirror(id, move))
                id += 1
                rebalance_steps.append(RebalanceStepSwitchoverToPrimary(id, move))
                id += 1
            else:
                rebalance_steps.append(RebalanceStepMoveMirror(id, move))
                id += 1

        # TODO: remove this dump
        #for step in rebalance_steps:
        #    self.logger.info(str(step))

        self.rebalance_schema.saveExecutionSteps(rebalance_steps)

        self.trigger('move_to_STATE_REBALANCE_PREPARE_MOVES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_STARTED(self) -> None:

        if self.rebalance_schema.allExecutionStepsAreDone():
            self.trigger('move_to_STATE_REBALANCE_EXECUTION_DONE')
            return

        rebalance_steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIERED])

        if len(rebalance_steps) > 0:

            if rebalance_steps[0].getStatus() == RebalanceStep.Status.APPROVE_REQUIERED:
                self.trigger('move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED')
                return

            moves = []
            switchover_step = None
            for step in rebalance_steps:
                if step.getStatus() == RebalanceStep.Status.APPROVE_REQUIERED:
                    break;

                # TODO: if we re-enter - some may be already IN_PROGRESS, so will will not see them in this list...
                step.setStatus(RebalanceStep.Status.IN_PROGRESS)
                self.rebalance_schema.updateExecutionStep(step)
                moves.append(step.getMove())
                if not isinstance(step, RebalanceStepMoveMirror):
                    switchover_step = step
                    break;

            if switchover_step != None:
                direction = self.RoleSwapDirection.PRIMARY_TO_MIRROR
                if not isinstance(switchover_step, RebalanceStepSwitchoverToMirror):
                    direction = self.RoleSwapDirection.MIRROR_TO_PRIMARY
                self.logger.info(f'Rebalance - start role swap {str(direction)}, segment {str(step.getMove().seg)}')
                self.execute_role_swaps([step.getMove().seg], direction)
                self.logger.info('Rebalance - end role swap')
            else:
                self.logger.info('Rebalance - start moving segments:')
                for move in moves:
                    self.logger.info(str(move))
                self.process_moves(moves)
                self.logger.info('Rebalance - end moving segments')

            # TODO: check the errored segments
            for step in rebalance_steps:
                if step.getStatus() == RebalanceStep.Status.IN_PROGRESS:
                    step.setStatus(RebalanceStep.Status.DONE)
                    self.rebalance_schema.updateExecutionStep(step)

        self.trigger('move_to_STATE_REBALANCE_MOVES_SUCCEEDED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVES_SUCCEEDED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED(self) -> None:
        rebalance_steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIERED])

        assert len(rebalance_steps) > 0

        step_to_approve = rebalance_steps[0]

        # TODO: we'll need to add logic here to get approval from the user in the interactive mode,
        # once we start implementing the interactive mode.
        # In non-interactive mode we assume that the switchover is always approved.
        step_to_approve.setStatus(RebalanceStep.Status.PLANNED)
        self.rebalance_schema.updateExecutionStep(step_to_approve)

        self.trigger('move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        pass

    # state callbacks end here
