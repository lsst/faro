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

import lsst.afw.table as afwTable

from lsst.faro.utils.coord_util import (averageRaFromCat,
                                        averageDecFromCat,
                                        averageRaDecFromCat,
                                        averageRaDec,
                                        sphDist)


class CoordUtilTest(unittest.TestCase):
    """Test coordinate utility functions."""

    def makeDataDiagonal(self):
        """Make minimal sythetic catalog with simple values."""
        ra_test = np.radians(np.linspace(-10., 10., 101))
        dec_test = np.radians(np.linspace(-10., 10., 101))
        schema = afwTable.SourceTable.makeMinimalSchema()
        cat = afwTable.SimpleCatalog(schema)
        cat.resize(len(ra_test))
        cat = cat.copy(deep=True)
        cat['coord_ra'][:] = ra_test
        cat['coord_dec'][:] = dec_test
        return cat

    def makeDataHorizontal(self):
        """Make minimal sythetic catalog with simple values."""
        ra_test = np.linspace(np.radians(-10.), np.radians(10.), 101)
        dec_test = np.tile(0., 101)
        schema = afwTable.SourceTable.makeMinimalSchema()
        cat = afwTable.SimpleCatalog(schema)
        cat.resize(len(ra_test))
        cat = cat.copy(deep=True)
        cat['coord_ra'][:] = ra_test
        cat['coord_dec'][:] = dec_test
        return cat

    def test_averageRaFromCat(self):
        """Test average RA calculation."""
        expected = 0.
        cat = self.makeDataDiagonal()
        result = averageRaFromCat(cat)
        self.assertAlmostEqual(result, expected, places=15)

    def test_averageDecFromCat(self):
        """Test average declination calculation."""
        expected = 0.
        cat = self.makeDataDiagonal()
        result = averageDecFromCat(cat)
        self.assertAlmostEqual(result, expected, places=15)

    def test_averageRaDecFromCat(self):
        """Test average RA and declination calculation."""
        expected = (0., 0.)
        cat = self.makeDataDiagonal()
        result = averageRaDecFromCat(cat)
        self.assertAlmostEqual(result[0], expected[0], places=15)
        self.assertAlmostEqual(result[1], expected[1], places=15)

    def test_averageRaDec(self):
        """Test average RA and declination calculation."""
        expected = (0., 0.)
        cat = self.makeDataDiagonal()
        result = averageRaDec(cat['coord_ra'], cat['coord_dec'])
        self.assertAlmostEqual(result[0], expected[0], places=15)
        self.assertAlmostEqual(result[1], expected[1], places=15)

    def test_sphDist(self):
        """Test great circle angular separation calculation."""
        expected = np.fabs(np.radians(np.linspace(-10., 10., 101)))
        cat = self.makeDataHorizontal()
        ra_mean, dec_mean = (0., 0.)
        result = sphDist(ra_mean, dec_mean, cat['coord_ra'], cat['coord_dec'])
        self.assertTrue(np.allclose(result, expected, atol=1.e-15))


if __name__ == "__main__":
    unittest.main()
