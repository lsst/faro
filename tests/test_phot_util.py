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
import os
import numpy as np
import astropy.units as u

from lsst.utils import getPackageDir
import lsst.pipe.base as pipeBase
from lsst.afw.table import SimpleCatalog, GroupView
from metric_pipeline_utils.phot_repeat import (calcPhotRepeat,
                                               calcPhotRepeatSample,
                                               computeWidths,
                                               getRandomDiffRmsInMmags,
                                               getRandomDiff)


DATADIR = os.path.join(getPackageDir('metric_pipeline_tasks'), 'tests', 'data')


class PhotUtilTest(unittest.TestCase):
    """Test photometric utility functions."""

    def load_data(self):
        '''Helper to load data to process.'''
        cat_file = 'matchedCatalogTract_0_i.fits.gz'
        catalog = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))
        matches = GroupView.build(catalog)

        magKey = matches.schema.find('slot_PsfFlux_mag').key
        nMatchesRequired = 2

        def nMatchFilter(cat):
            if len(cat) < nMatchesRequired:
                return False
            return np.isfinite(cat.get(magKey)).all()

        return matches.where(nMatchFilter), magKey

    def test_calcPhotRepeat(self):
        """Test photometric repeatability for multiple realizations
        of random pairs of visits."""
        # Ensure measurements are deterministic
        seed = 8675309

        expected = 72.79576602821936 * u.mmag
        matches, magKey = self.load_data()
        result = calcPhotRepeat(matches, magKey, randomSeed=seed)
        print('LOOK HERE', result['repeatability'])
        self.assertEqual(result['repeatability'], expected)

    def test_calcPhotRepeatSample(self):
        """Test photometric repeatability for one realization
        of random pairs of visits."""
        # Ensure measurements are deterministic
        seed = 8675309
        rng = np.random.default_rng(seed)

        expected = pipeBase.Struct(rms=168.09034424742475,
                                   iqr=72.11774706688344)
        matches, magKey = self.load_data()
        result = calcPhotRepeatSample(matches, magKey, rng=rng)
        self.assertEqual(result.rms, expected.rms)
        self.assertEqual(result.iqr, expected.iqr)

    def test_computeWidths(self):
        """Test RMS and the scaled inter-quartile range calculation."""
        expected = (22.54717277176897, 1.8532527731320025)
        mag = np.linspace(20, 25, 101)
        result = computeWidths(mag)
        self.assertEqual(result[0], expected[0])
        self.assertEqual(result[1], expected[1])

    def test_getRandomDiffRmsInMmags(self):
        """Test random sampling of magnitude diffs."""
        # Ensure measurements are deterministic
        seed = 8675309
        rng = np.random.default_rng(seed)

        expected = -1237.436867076458
        mag = np.linspace(20, 25, 101)
        result = getRandomDiffRmsInMmags(mag, rng=rng)
        self.assertEqual(result, expected)

    def test_getRandomDiff(self):
        """Test one random diff"""
        # Ensure measurements are deterministic
        seed = 8675309
        rng = np.random.default_rng(seed)

        expected = -1.75
        mag = np.linspace(20, 25, 101)
        result = getRandomDiff(mag, rng=rng)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
