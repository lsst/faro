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
import os
import numpy as np

from lsst.utils import getPackageDir
from lsst.afw.table import SimpleCatalog
from lsst.faro.utils.tex import (TraceSize, PsfTraceSizeDiff,
                                 E1, E2, E1Resids, E2Resids,
                                 RhoStatistics)


DATADIR = os.path.join(getPackageDir('faro'), 'tests', 'data')


class TEXUtilTest(unittest.TestCase):
    """Test TEX utility functions."""

    def loadData(self):
        """Helper to load data to process."""
        cat_file = 'src_HSC_i_HSC-I_903986_0_31_HSC_runs_ci_hsc_20210407T021858Z.fits'
        cat = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))

        # selection = np.isfinite(cat.get('e1')) & np.isfinite(cat.get('e2'))
        selection = np.tile(True, len(cat))

        return cat.subset(selection).copy(deep=True)

    def testEllipticityDefinitions(self):
        """Test ellipticity functors."""

        cat = self.loadData()

        column = 'slot_Shape'
        columnPsf = 'slot_PsfShape'

        trace = TraceSize(column)
        result = np.nanmean(trace(cat))
        expected = 4.36377821335775
        self.assertEqual(result, expected)

        traceDiff = PsfTraceSizeDiff(column, columnPsf)
        result = np.nanmean(traceDiff(cat))
        expected = 25.301812428201995
        self.assertEqual(result, expected)

        e1 = E1(column)
        result = np.nanmean(e1(cat))
        expected = 0.0012636175684993878
        self.assertEqual(result, expected)

        e1 = E1(column, shearConvention=True)
        result = np.nanmean(e1(cat))
        expected = 0.00043009504274617235
        self.assertEqual(result, expected)

        e2 = E2(column)
        result = np.nanmean(e2(cat))
        expected = 0.080076033827269
        self.assertEqual(result, expected)

        e2 = E2(column, shearConvention=True)
        result = np.nanmean(e2(cat))
        expected = 0.04194134295796996
        self.assertEqual(result, expected)

        e1Resids = E1Resids(column, columnPsf)
        result = np.nanmean(e1Resids(cat))
        expected = -0.0009098947676481413
        self.assertEqual(result, expected)

        e2Resids = E2Resids(column, columnPsf)
        result = np.nanmean(e2Resids(cat))
        expected = -0.02280606766168935
        self.assertEqual(result, expected)

    def testRhoStats(self):
        """Compute six Rho statistics."""

        cat = self.loadData()
        column = 'slot_Shape'
        columnPsf = 'slot_PsfShape'

        treecorrKwargs = dict(nbins=5,
                              min_sep=0.25,
                              max_sep=1,
                              sep_units='arcmin',
                              brute=True)
        rhoStatistics = RhoStatistics(column, columnPsf, **treecorrKwargs)
        result = rhoStatistics(cat)

        expected = [0.2344471639428089,
                    0.0010172306766334468,
                    -0.0021045081490440086,
                    0.0028001762296714734,
                    -0.0013278190624450364,
                    0.005010295952053786]

        self.assertAlmostEqual(np.mean(result[0].xi), expected[0], places=7)
        self.assertAlmostEqual(np.mean(result[1].xip), expected[1], places=7)
        self.assertAlmostEqual(np.mean(result[2].xip), expected[2], places=7)
        self.assertAlmostEqual(np.mean(result[3].xip), expected[3], places=7)
        self.assertAlmostEqual(np.mean(result[4].xip), expected[4], places=7)
        self.assertAlmostEqual(np.mean(result[5].xip), expected[5], places=7)


if __name__ == "__main__":
    unittest.main()
