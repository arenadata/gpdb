import random
from typing import List, Dict, Tuple
from collections import defaultdict

from gppylib.test.unit.gp_unittest import *
from ..solver import GreedySolver
from .config import getEncoding


class TestGreedySolver(GpTestCase):

    def setUp(self):
        random.seed(42)
    
    def _validate_solution_allassign(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver) -> bool:
        if len(solution) != solver.n_segments:
            return False

        return True    
    def _validate_solution_host_ids(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if not (0 <= primary < solver.n_hosts_target):
                return False
            if not (0 <= mirror < solver.n_hosts_target):
                return False
        
        return True
    
    def _validate_solution_nocolocation(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if primary == mirror:
                return False
        return True
    
    def _validate_solution_balance(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver) -> bool:
        load = [0] * solver.n_hosts_target
        for seg_id, (primary, mirror) in solution.items():
            load[primary] += 1
            load[mirror] += 1
        
        for host, host_load in enumerate(load):
            if host_load != solver.target_load:
                return False
        
        return True
    
    def _validate_strategy(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver):
        primary_to_mirrors = defaultdict(list)
        for seg_id, (primary, mirror) in solution.items():
            primary_to_mirrors[primary].append(mirror)
        
        for primary, mirrors in primary_to_mirrors.items():
            if solver.strategy == 'grouped':
                unique_mirrors = set(mirrors)
                if len(unique_mirrors) > 1:
                    return False
            elif solver.strategy == 'spread':
                unique_mirrors = set(mirrors)
                if len(mirrors) != len(unique_mirrors):
                    return False
        return True
    
    def _validate_solition(self, solution: Dict[int, Tuple[int, int]], 
                                     solver: GreedySolver):
        self.assertTrue(self._validate_solution_allassign(solution, solver),
                        f"Missing segments. Expected {solver.n_segments}, got {len(solution)}")
        
        self.assertTrue(self._validate_solution_host_ids(solution, solver),
                        "Some segments has impossible host assignment")

        self.assertTrue(self._validate_solution_nocolocation(solution, solver),
                        "Some segment has primary==mirror assignment")
        
        self.assertTrue(self._validate_solution_balance(solution, solver),
                        f"One of hosts has load different from expected {solver.target_load}")
        
        self.assertTrue(self._validate_strategy(solution, solver),
                        "Grouped mirroring strategy is violated")

    @getEncoding('35_7_balanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_balanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 0)

        self._validate_solition(solution, solver)
    
    @getEncoding('35_7_balanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_balanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 0)

        self._validate_solition(solution, solver)

    @getEncoding('40_5_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 21)

        self._validate_solition(solution, solver)

    @getEncoding('40_5_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_neg(self):
        conf = self.encoding[0]

        with self.assertRaises(ValueError, msg='Cannot follow spread mirroring strategy') as cm:
            solver = GreedySolver(*conf, run_improve=False)
    
    @getEncoding('40_8_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 12)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'spread', None, None, None)
    def test_validity_medium_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 20)

        self._validate_solition(solution, solver)

    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 337)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 528)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        # means didn't find any better
        self.assertEqual(cost, 337)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        # initial solution gives 528
        self.assertEqual(cost, 330)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 103)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'grouped', None, None, None)
    def test_strategy_change_medium_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 106)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_gropued_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 103)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts="sdw13, sdw14, sdw15, sdw16, sdw17, sdw18, sdw19, sdw20")
    def test_decomission_hosts(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 117)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
    add_hosts="sdw21, sdw22, sdw23, sdw24, sdw25, sdw26, sdw27, sdw28, sdw29, sdw30",
    remove_hosts=None)
    def test_new_hosts(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 84)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=
                 "sdw1, sdw2, sdw3, sdw4, sdw5, sdw21, sdw22, sdw23, sdw24, sdw25",
                 add_hosts=None, remove_hosts=None)
    def test_target_hosts(self):
        conf = self.encoding[0]

        solver = GreedySolver(*conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 176)

        self._validate_solition(solution, solver)
    
if __name__ == '__main__':
    run_tests()
