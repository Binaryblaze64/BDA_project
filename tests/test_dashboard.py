import importlib.util, math, unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('dashboard_server',Path(__file__).resolve().parents[1]/'dashboard/server.py')
server=importlib.util.module_from_spec(spec);spec.loader.exec_module(server)
class ValidationTests(unittest.TestCase):
    def setUp(self):self.row=dict(energy_kcal=250,fat=8,saturated_fat=2,sugars=5,fiber=6,proteins=10,salt=.6)
    def test_valid_single_and_comparison(self):
        for count in (1,2):self.assertEqual(len(server.validate_products({'products':[self.row]*count})),count)
    def test_missing_and_nonfinite_values(self):
        for value in (None,'8',True,float('nan'),float('inf'),-1,101):
            row=dict(self.row,fat=value)
            with self.subTest(value=value),self.assertRaises(ValueError):server.validate_products({'products':[row]})
    def test_energy_uses_separate_units(self):
        self.assertEqual(server.validate_products({'products':[dict(self.row,energy_kcal=900)]})[0]['energy_kcal'],900)
        with self.assertRaises(ValueError):server.validate_products({'products':[dict(self.row,energy_kcal=901)]})
    def test_saturated_fat_and_mass_constraints(self):
        for row in (dict(self.row,saturated_fat=9),dict(self.row,sugars=90)):
            with self.assertRaises(ValueError):server.validate_products({'products':[row]})
    def test_batch_limits_and_structure(self):
        for body in (None,[],{}, {'products':[]},{'products':[self.row]*3},{'products':[None]}):
            with self.assertRaises(ValueError):server.validate_products(body)
if __name__=='__main__':unittest.main()
