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

"""Unit tests for the metrics measurement system: wPerp, extinction_corr.
"""

import unittest
import numpy as np
import os
import astropy.units as u

from lsst.utils import getPackageDir
from lsst.afw.table import SimpleCatalog
from metric_pipeline_utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats
from astropy.coordinates import SkyCoord
try:
    from dustmaps.sfd import SFDQuery
except ModuleNotFoundError:
    print("The extinction_corr method is not available without first installing the dustmaps module:\n"
          "$> pip install --user dustmaps\n\n"
          "Then in a python interpreter:\n"
          ">>> import dustmaps.sfd\n"
          ">>> dustmaps.sfd.fetch()\n")


DATADIR = os.path.join(getPackageDir('metric_pipeline_tasks'), 'tests', 'data')


class StellarLocusTest(unittest.TestCase):

    def load_data(self, key):
        '''Helper to load data to process.'''
        cat_file = 'tract9813_patches55to72_gri_filtered.fits.gz'
        catalog = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))
        return catalog

    def test_extinction_corr(self):
        """Test lookup of extinction corrections on a known catalog."""
        cat = self.load_data()
        coords = SkyCoord(cat['coord_ra_r'], cat['coord_dec_r'])
        sfd = SFDQuery()
        ebvValues = sfd(coords)
        expected_mean_ebv = 0.083 * u.mag
        expected_len_ebv = 207
        self.assertEqual(np.mean(ebvValues), expected_mean_ebv)
        self.assertEqual(len(ebvValues), expected_len_ebv)

    def test_wPerp(self):
        """Test calculation of wPerp (stellar locus metric) on a known catalog."""
        cat = self.load_data()
        expected_wperp = 12.13129298 * u.mmag
        coords = SkyCoord(cat['coord_ra_r'], cat['coord_dec_r'])
        sfd = SFDQuery()
        ebvValues = sfd(coords)
        p1, p2, p1coeffs, p2coeffs = stellarLocusResid(cat['base_PsfFlux_mag_g']-ebvValues['A_g'],
                                                       cat['base_PsfFlux_mag_r']-ebvValues['A_r'],
                                                       cat['base_PsfFlux_mag_i']-ebvValues['A_i'])
        p2_rms = calcQuartileClippedStats(p2).rms*u.mag
        self.assertEqual(p2_rms.to(u.mmag), expected_wperp)

    @classmethod
    def tearDownClass(cls):
        '''Delete any variables set in setUpClass.'''
        del cls.file_map
        super().tearDownClass()


if __name__ == "__main__":
    unittest.main()
