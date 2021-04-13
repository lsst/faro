# This file is part of <REPLACE WHEN RENAMED>.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for the metrics measurement system: configs.
"""

import unittest

from lsst.faro.measurement import (AMxTask, ADxTask, AFxTask,
                                   PA1Task, PA2Task, PF1Task,
                                   TExTask, AB1Task, WPerpTask)


class ConfigTest(unittest.TestCase):

    def check_config(self, task, expected, default, check_fields):
        """This checks both that there are attributes on the task
           that match the expected configuration as well as that the
           configuration is different than the default config.
        """
        for field in check_fields:
            self.assertEqual(getattr(task.config, field), getattr(expected, field))
            self.assertNotEqual(getattr(task.config, field), getattr(default, field))

    # AMxTask, ADxTask, and AFxTask share a config so we are really only testing
    # how the tasks apply the config
    def test_amx_config(self):
        """Test application of config for AMx task"""
        default = AMxTask.ConfigClass()
        expected = AMxTask.ConfigClass()
        field_list = ['annulus_r', 'width', 'bright_mag_cut', 'faint_mag_cut',
                      'threshAD', 'threshAF', 'bins']
        expected.annulus_r = 11.7
        expected.width = 3.33
        expected.bright_mag_cut = 20.0
        expected.faint_mag_cut = 23.8
        expected.threshAD = 19.63
        expected.threshAF = 11.55
        expected.bins = [0.5, 1.6, 3.1, 2.8]
        task = AMxTask(config=expected)
        self.check_config(task, expected, default, field_list)

    def test_adx_config(self):
        """Test application of config for ADx task"""
        default = ADxTask.ConfigClass()
        expected = ADxTask.ConfigClass()
        field_list = ['annulus_r', 'width', 'bright_mag_cut', 'faint_mag_cut',
                      'threshAD', 'threshAF', 'bins']
        expected.annulus_r = 11.7
        expected.width = 3.33
        expected.bright_mag_cut = 20.0
        expected.faint_mag_cut = 23.8
        expected.threshAD = 19.63
        expected.threshAF = 11.55
        expected.bins = [0.5, 1.6, 3.1, 2.8]
        task = ADxTask(config=expected)
        self.check_config(task, expected, default, field_list)

    def test_afx_config(self):
        """Test application of config for AFx task"""
        default = AFxTask.ConfigClass()
        expected = AFxTask.ConfigClass()
        field_list = ['annulus_r', 'width', 'bright_mag_cut', 'faint_mag_cut',
                      'threshAD', 'threshAF', 'bins']
        expected.annulus_r = 11.7
        expected.width = 3.33
        expected.bright_mag_cut = 20.0
        expected.faint_mag_cut = 23.8
        expected.threshAD = 19.63
        expected.threshAF = 11.55
        expected.bins = [0.5, 1.6, 3.1, 2.8]
        task = AFxTask(config=expected)
        self.check_config(task, expected, default, field_list)

    def test_pa1_config(self):
        """Test application of config for PA1 task"""
        default = PA1Task.ConfigClass()
        expected = PA1Task.ConfigClass()
        field_list = ['brightSnrMin', 'brightSnrMax']
        expected.brightSnrMin = 100.0
        expected.brightSnrMax = 31415.9
        task = PA1Task(config=expected)
        # Some config attriutes are also used to populate task attriubtes
        self.assertEqual(task.config.brightSnrMin, expected.brightSnrMin)
        self.assertEqual(task.config.brightSnrMax, expected.brightSnrMax)
        self.check_config(task, expected, default, field_list)

    def test_pa2_config(self):
        """Test application of config for PA2 task"""
        default = PA2Task.ConfigClass()
        expected = PA2Task.ConfigClass()
        field_list = ['brightSnrMin', 'brightSnrMax']
        expected.brightSnrMin = 100.0
        expected.brightSnrMax = 31415.9
        task = PA2Task(config=expected)
        # Some config attriutes are also used to populate task attriubtes
        self.assertEqual(task.config.brightSnrMin, expected.brightSnrMin)
        self.assertEqual(task.config.brightSnrMax, expected.brightSnrMax)
        self.check_config(task, expected, default, field_list)

    def test_pf1_config(self):
        """Test application of config for PF1 task"""
        default = PF1Task.ConfigClass()
        expected = PF1Task.ConfigClass()
        field_list = ['brightSnrMin', 'brightSnrMax', 'threshPA2', 'threshPF1']
        expected.brightSnrMin = 100.0
        expected.brightSnrMax = 31415.9
        expected.threshPA2 = 17.47
        expected.threshPF1 = 9.892
        task = PF1Task(config=expected)
        # Some config attriutes are also used to populate task attriubtes
        self.assertEqual(task.config.brightSnrMin, expected.brightSnrMin)
        self.assertEqual(task.config.brightSnrMax, expected.brightSnrMax)
        self.assertEqual(task.config.threshPA2, expected.threshPA2)
        self.assertEqual(task.config.threshPF1, expected.threshPF1)
        self.check_config(task, expected, default, field_list)

    def test_tex_config(self):
        """Test application of config for TEx task"""
        default = TExTask.ConfigClass()
        expected = TExTask.ConfigClass()
        #field_list = ['annulus_r', 'comparison_operator']
        #expected.annulus_r = 42.0
        #expected.comparison_operator = 'bigger'
        field_list = ['min_sep', 'max_sep', 'nbins', 
                      'rho_stat', 'ellipticity_convention', 'column_psf', 'column']
        expected.min_sep = 5.
        expected.max_sep = 20.
        expected.nbins = 100
        expected.rho_stat = 2
        expected.ellipticity_convention = 'shear'
        expected.column_psf = 'base_SdssShape_psf'
        expected.column = 'base_SdssShape'
        task = TExTask(config=expected)
        self.check_config(task, expected, default, field_list)

    def test_ab1_config(self):
        """Test application of config for AB1 task"""
        default = AB1Task.ConfigClass()
        expected = AB1Task.ConfigClass()
        field_list = ['bright_mag_cut', 'faint_mag_cut', 'ref_filter']
        expected.bright_mag_cut = 25.3
        expected.faint_mag_cut = 38.2
        expected.ref_filter = 'p'
        task = AB1Task(config=expected)
        self.check_config(task, expected, default, field_list)

    def test_wperp_config(self):
        """Test application of config for wPerp task"""
        default = WPerpTask.ConfigClass()
        expected = WPerpTask.ConfigClass()
        field_list = ['bright_rmag_cut', 'faint_rmag_cut']
        expected.bright_rmag_cut = 25.3
        expected.faint_rmag_cut = 38.2
        task = WPerpTask(config=expected)
        self.check_config(task, expected, default, field_list)


if __name__ == "__main__":
    unittest.main()
