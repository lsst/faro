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
import operator
import astropy.units as u

from lsst.utils import getPackageDir
from lsst.afw.table import SimpleCatalog, GroupView
from lsst.faro.utils.coord_util import averageRaFromCat, averageDecFromCat
from lsst.faro.utils.tex import (correlation_function_ellipticity,
                                 select_bin_from_corr,
                                 medianEllipticity1ResidualsFromCat,
                                 medianEllipticity2ResidualsFromCat)

DATADIR = os.path.join(getPackageDir('faro'), 'tests', 'data')


class TEXUtilTest(unittest.TestCase):
    """Test TEX utility functions."""

    def load_data(self):
        '''Helper to load data to process.'''
        cat_file = 'matchedCatalogTract_0_i.fits.gz'
        cat = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))

        selection = np.isfinite(cat.get('e1')) & np.isfinite(cat.get('e2'))

        return cat.subset(selection).copy(deep=True)

    def test_correlation_function_ellipticity(self):
        """Test correlation function calculation."""
        expected_r = 4.395319409351799*u.arcmin
        expected_xip = 0.001867305310370419
        expected_xip_err = 0.0005348912968829396

        cat = self.load_data()
        matches = GroupView.build(cat)

        ra = matches.aggregate(averageRaFromCat) * u.radian
        dec = matches.aggregate(averageDecFromCat) * u.radian

        e1_res = matches.aggregate(medianEllipticity1ResidualsFromCat)
        e2_res = matches.aggregate(medianEllipticity2ResidualsFromCat)

        result = correlation_function_ellipticity(ra, dec, e1_res, e2_res)
        self.assertTrue(u.isclose(np.mean(result[0]), expected_r))
        self.assertTrue(u.isclose(np.mean(result[1]), expected_xip))
        self.assertTrue(u.isclose(np.mean(result[2]), expected_xip_err))

    def test_select_bin_from_corr(self):
        """Test selection of angular range from correlation function."""

        # Test less than or equal to logic
        expected = (4.666666666666667, 1.3820881233139908)
        radius = np.arange(1, 11) * u.arcmin
        xip = radius.value**2
        xip_err = np.sqrt(radius.value)
        result = select_bin_from_corr(radius, xip, xip_err, radius=3.*u.arcmin, operator=operator.le)
        self.assertEqual(result, expected)

        # Test greater than or equal to logic
        expected = (47.5, 2.506758077978876)
        radius = np.arange(1, 11) * u.arcmin
        xip = radius.value**2
        xip_err = np.sqrt(radius.value)
        result = select_bin_from_corr(radius, xip, xip_err, radius=3.*u.arcmin, operator=operator.ge)
        self.assertEqual(result, expected)

    def test_medianEllipticity1ResidualsFromCat(self):
        """Test ellipticity residuals e1."""
        expected = -0.004235647618770565
        cat = self.load_data()
        result = medianEllipticity1ResidualsFromCat(cat)
        self.assertEqual(result, expected)

    def test_medianEllipticity2ResidualsFromCat(self):
        """Test ellipticity residuals e2."""
        expected = 0.0008848644793033184
        cat = self.load_data()
        result = medianEllipticity2ResidualsFromCat(cat)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
