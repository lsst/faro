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
from astropy.table import Table
from lsst.faro.utils.stellar_locus import stellarLocusResid
from lsst.faro.utils.extinction_corr import extinction_corr
from lsst.faro.measurement import WPerpTask


DATADIR = os.path.join(getPackageDir('faro'), 'tests', 'data')


class StellarLocusTest(unittest.TestCase):

    def load_data(self):
        '''Helper to load data to process.'''
        cat_file = 'tract9813_patches55to72_gri_filtered.fits.gz'
        catalog = Table.read(os.path.join(DATADIR, cat_file))
        return catalog

    def test_extinction_corr(self):
        """Test lookup of extinction corrections on a known catalog."""
        cat = self.load_data()
        # 'r' must come first, because the coord column is named 'coord_ra_r', and
        # extinction_corr assumes the first bandpass provided is used for the coord column.
        bands = ['r', 'g', 'i']
        ext_vals = extinction_corr(cat, bands)
        ebvValues = ext_vals['E(B-V)']
        expected_mean_ebv = 0.020206410437822342
        expected_len_ebv = 209
        self.assertEqual(np.mean(ebvValues), expected_mean_ebv)
        self.assertEqual(len(ebvValues), expected_len_ebv)

    def test_wPerp(self):
        """Test calculation of wPerp (stellar locus metric) on a known catalog."""
        cat = self.load_data()
        bands = ['r', 'g', 'i']
        ext_vals = extinction_corr(cat, bands)

        expected_wperp = 12.18208045737346 * u.mmag

        config = WPerpTask.ConfigClass()
        task = WPerpTask(config=config)
        result = task.calcWPerp(cat, ext_vals, 'wPerp')
        self.assertEqual(result.measurement.quantity, expected_wperp)

    def test_stellarLocusResid(self):
        """Test calculation of stellar locus residuals on a known catalog."""
        cat = self.load_data()
        bands = ['r', 'g', 'i']
        ext_vals = extinction_corr(cat, bands)

        p1, p2, p1coeffs, p2coeffs = stellarLocusResid(cat['base_PsfFlux_mag_g']-ext_vals['A_g'],
                                                       cat['base_PsfFlux_mag_r']-ext_vals['A_r'],
                                                       cat['base_PsfFlux_mag_i']-ext_vals['A_i'])

        expected_p1coeffs = [0.0, 0.8865855025842218, -0.424020754027349,
                             -0.46256474855687274, 0.0, -0.3073194572752734]
        expected_p2coeffs = [0.0, -0.2754432193878283, 0.8033778833591981,
                             -0.5279346639713699, 0.0, 0.03544642888292535]
        expected_p1median = 0.1775458238071685
        expected_p2median = 4.766061008374539e-05
        self.assertEqual(np.median(p1), expected_p1median)
        self.assertEqual(np.median(p2), expected_p2median)
        self.assertEqual(p1coeffs, expected_p1coeffs)
        self.assertEqual(p2coeffs, expected_p2coeffs)


if __name__ == "__main__":
    unittest.main()
