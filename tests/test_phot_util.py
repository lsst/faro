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
import astropy.units as u

from lsst.utils import getPackageDir
from lsst.afw.table import SimpleCatalog, GroupView
from lsst.faro.utils.phot_repeat import calcPhotRepeat

DATADIR = os.path.join(getPackageDir('faro'), 'tests', 'data')


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

        expected = 56.069125538189226 * u.mmag
        matches, magKey = self.load_data()
        result = calcPhotRepeat(matches, magKey)
        self.assertEqual(result['repeatability'], expected)


if __name__ == "__main__":
    unittest.main()
