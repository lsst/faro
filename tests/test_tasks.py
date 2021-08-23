# This file is part of faro.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import unittest
import astropy.units as u

from lsst.utils import getPackageDir
from lsst.afw.table import SimpleCatalog

from lsst.faro.base import CatalogMeasurementBaseConfig, CatalogMeasurementBaseTask, NumSourcesMergeTask
from lsst.faro.measurement import (VisitTableMeasurementConfig, VisitTableMeasurementTask,
                                   DetectorTableMeasurementConfig, DetectorTableMeasurementTask,
                                   VisitMeasurementConfig, VisitMeasurementTask,
                                   DetectorMeasurementConfig, DetectorMeasurementTask,
                                   TractMeasurementConfig, TractMeasurementTask,)

DATADIR = os.path.join(getPackageDir('faro'), 'tests', 'data')


class TaskTest(unittest.TestCase):

    def load_data(self, key):
        cat_file = self.file_map[key]
        catalog = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))
        return catalog

    def setUp(self):
        """This is called immediately before calling each test method."""
        self.file_map = {'CatalogMeasurementBaseTask':
                         'src_HSC_i_HSC-I_903986_0_31_HSC_runs_ci_hsc_20210407T021858Z.fits'}

    def testCatalogMeasurementBaseTask(self):
        """Test run method of CatalogMeasurementBaseTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = CatalogMeasurementBaseConfig()
        t = CatalogMeasurementBaseTask(config)
        outputs = t.run(catalog=catalog)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testVisitTableMeasurementTask(self):
        """Test run method of VisitTableMeasurementTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = VisitTableMeasurementConfig()
        t = VisitTableMeasurementTask(config)
        outputs = t.run(catalog=catalog)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testDetectorTableMeasurementTask(self):
        """Test run method of VisitTableMeasurementTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = DetectorTableMeasurementConfig()
        t = DetectorTableMeasurementTask(config)
        outputs = t.run(catalog=catalog)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testTractMeasurementTask(self):
        """Test run method of TractMeasurementTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = TractMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        t = TractMeasurementTask(config)
        outputs = t.run(
            catalogs=[catalog, ],
            photoCalibs=[None, ],
            astromCalibs=[None, ],
            dataIds=[{'band': 'r'}, ],
        )
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testVisitMeasurementTask(self):
        """Test run method of VisitMeasurementTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = VisitMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        t = VisitMeasurementTask(config)
        outputs = t.run(
            catalogs=[catalog, ],
            photoCalibs=[None, ],
            astromCalibs=[None, ],
            dataIds=[{'band': 'r'}, ],
        )
        print(outputs)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testDetectorMeasurementTask(self):
        """Test run method of DetectorMeasurementTask."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = DetectorMeasurementConfig()
        t = DetectorMeasurementTask(config)
        outputs = t.run(catalog=catalog)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)


if __name__ == "__main__":
    unittest.main()
