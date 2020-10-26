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

"""Unit tests for the metrics measurement system.
"""

import unittest
import numpy as np
import random

from metric_pipeline_utils.phot_repeat import (getRandomDiffRmsInMmags,
                                               computeWidths)

# Make sure measurements are deterministic
random.seed(8675309)


class PhotUtilTest(unittest.TestCase):
    """Test photometric utility functions."""
    
    def test_computeWidths(self):
        """Test RMS and the scaled inter-quartile range calculation."""
        expected = (22.54717277176897, 1.8532527731320025)
        mag = np.linspace(20, 25, 101)
        result = computeWidths(mag)
        self.assertEqual(result[0], expected[0])
        self.assertEqual(result[1], expected[1])
    
    def test_getRandomDiffRmsInMmags(self):
        """Test random sampling of magnitude diffs."""
        expected = 388.9087296526016
        mag = np.linspace(20, 25, 101)
        result = getRandomDiffRmsInMmags(mag)
        self.assertEqual(result, expected)

        
if __name__ == "__main__":
    unittest.main()