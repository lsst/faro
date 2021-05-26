# This file is part of faro.
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

"""Unit tests for the metrics measurement system.
"""

import unittest
import numpy as np
import astropy.units as u

from lsst.faro.measurement import HistMedianTask
from lsst.pex.config import Config


class MockMeas:
    def __init__(self, extras):
        self.extras = extras


class MockExtra:
    def __init__(self, quantity):
        self.quantity = quantity


class HistMedianTest(unittest.TestCase):

    def test_hist_mode(self):
        """Test the aggregator that takes the mode of a histogram."""
        task = HistMedianTask(config=Config())
        values = np.array([0, 2, 4, 6, 8, 10, 14, 12, 16, 12, 8, 4, 0])*u.count
        bins = np.array([(i+1)*0.3 for i in range(len(values)+1)])*u.m
        expected = 2.5*u.m
        mock_meas_arr = []
        for i in range(37):
            extras = {}
            extras['values'] = MockExtra(values*np.random.randint(1, 29))
            extras['bins'] = MockExtra(bins)
            mock_meas_arr.append(MockMeas(extras))
        result = task.run(mock_meas_arr, 'test', 'info', 'mock')
        self.assertEqual(expected, result.measurement.quantity)


if __name__ == "__main__":
    unittest.main()
