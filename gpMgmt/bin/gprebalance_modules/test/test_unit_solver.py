import random
from typing import List, Dict, Tuple
from collections import defaultdict

from gppylib.test.unit.gp_unittest import *
from ..solver import GreedySolver, Solution
from .config import getEncoding

class TestGreedySolver(GpTestCase):

    def setUp(self):
        random.seed(42)
    
    def _validate_solution_allassign(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        if len(solution) != solver.n_segments:
            return False

        return True    
    def _validate_solution_host_ids(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if not (0 <= primary < solver.n_hosts_target):
                return False
            if not (0 <= mirror < solver.n_hosts_target):
                return False
        
        return True
    
    def _validate_solution_nocolocation(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if primary == mirror:
                return False
        return True
    
    def _validate_solution_balance(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        load = [0] * solver.n_hosts_target
        for seg_id, (primary, mirror) in solution.items():
            load[primary] += 1
            load[mirror] += 1
        
        for host, host_load in enumerate(load):
            if host_load != solver.target_load:
                return False
        
        return True
    
    def _validate_strategy(self, solution: Solution, 
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
    
    def _validate_solition(self, solution: Solution, 
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

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 0)

        self._validate_solition(solution, solver)
    
    @getEncoding('35_7_balanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_balanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 0)

        self._validate_solition(solution, solver)

    @getEncoding('40_5_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 19)

        self._validate_solition(solution, solver)
    
    @getEncoding('40_5_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 18)

        self._validate_solition(solution, solver)

    @getEncoding('40_5_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_neg(self):
        conf = self.encoding[0]

        with self.assertRaises(ValueError, msg='Cannot follow spread mirroring strategy') as cm:
            solver = GreedySolver(conf, run_improve=False)
    
    @getEncoding('40_8_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 10)

        self._validate_solition(solution, solver)
    
    @getEncoding('40_8_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 6)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'spread', None, None, None)
    def test_validity_medium_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 10)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'spread', None, None, None)
    def test_validity_medium_spread_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 7)

        self._validate_solition(solution, solver)

    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 124)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()
        
        # in more or less standart configurations with lightly skewed
        # distribution greedy initial solution is pretty-well generated.
        # it's expected that ALNS may not bring any impovements.
        self.assertEqual(cost, 124)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 140)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        # in standart configurations with lightly skewed
        # distribution greedy initial solution is pretty-well generated.
        # it's expected that ALNS may not bring any impovements.
        self.assertEqual(cost, 140)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_grouped_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 101)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_grouped_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 100)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'grouped', None, None, None)
    def test_strategy_change_medium_spread_unbalanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 106)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_spread', 'grouped', None, None, None)
    def test_strategy_change_medium_spread_unbalanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 102)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts="sdw13, sdw14, sdw15, sdw16, sdw17, sdw18, sdw19, sdw20")
    def test_decomission_hosts(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 116)

        self._validate_solition(solution, solver)

    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts="sdw13, sdw14, sdw15, sdw16, sdw17, sdw18, sdw19, sdw20")
    def test_decomission_hosts_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 109)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts=",".join(["sdw" + str(i) for i in range(20, 30)]))
    def test_decomission_hosts_large(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 470)

        self._validate_solition(solution, solver)

    @getEncoding('1000_50_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts=",".join(["sdw" + str(i) for i in range(20, 30)]))
    def test_decomission_hosts_large_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 457)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_balanced_grouped', 'grouped', target_hosts=None,
    add_hosts=",".join(["sdw" + str(i) for i in range(51, 101)]),
    remove_hosts=None)
    def test_new_hosts_balanced(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        #optimal cost
        self.assertEqual(cost, 1000)

        self._validate_solition(solution, solver)
    
    @getEncoding('1000_50_balanced_grouped', 'grouped', target_hosts=None,
    add_hosts=",".join(["sdw" + str(i) for i in range(51, 101)]),
    remove_hosts=None)
    def test_new_hosts_balanced_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=True)

        solution, cost = solver.solve()

        #optimal cost
        self.assertEqual(cost, 1000)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=
                 "sdw1, sdw2, sdw3, sdw4, sdw5, sdw21, sdw22, sdw23, sdw24, sdw25, sdw12, sdw13",
                 add_hosts=None, remove_hosts=None)
    def test_target_hosts(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf, run_improve=False)

        solution, cost = solver.solve()

        self.assertEqual(cost, 175)

        self._validate_solition(solution, solver)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=
                 "sdw1, sdw2, sdw3, sdw4, sdw5, sdw21, sdw22, sdw23, sdw24, sdw25, sdw12, sdw13",
                 add_hosts=None, remove_hosts=None)
    def test_target_hosts_with_improve(self):
        conf = self.encoding[0]

        solver = GreedySolver(conf)

        solution, cost = solver.solve()

        self.assertEqual(cost, 160)

        self._validate_solition(solution, solver)
    
if __name__ == '__main__':
    run_tests()
