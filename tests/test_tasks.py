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

import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.geom as geom

from lsst.afw.table import SimpleCatalog

from lsst.faro.base import CatalogMeasurementBaseConfig, CatalogMeasurementBaseTask, NumSourcesMergeTask
from lsst.faro.measurement import (VisitTableMeasurementConfig, VisitTableMeasurementTask,
                                   DetectorTableMeasurementConfig, DetectorTableMeasurementTask,
                                   VisitMeasurementConfig, VisitMeasurementTask,
                                   DetectorMeasurementConfig, DetectorMeasurementTask,
                                   TractMeasurementConfig, TractMeasurementTask,
                                   TractTableValueMeasurementConfig, TractTableValueMeasurementTask,
                                   )

TESTDIR = os.path.abspath(os.path.dirname(__file__))
DATADIR = os.path.join(TESTDIR, 'data')


class TaskTest(unittest.TestCase):

    def load_data(self, key):
        cat_file = self.file_map[key]
        catalog = SimpleCatalog.readFits(os.path.join(DATADIR, cat_file))
        return catalog

    def setUp(self):
        """This is called immediately before calling each test method."""
        self.file_map = {'CatalogMeasurementBaseTask':
                         'src_HSC_i_HSC-I_903986_0_31_HSC_runs_ci_hsc_20210407T021858Z.fits'}
        # Make a mock wcs
        self.mockWcs = afwGeom.makeSkyWcs(
            crpix=geom.Point2D(1, 1),
            crval=geom.SpherePoint(0.0*geom.degrees, 90.0*geom.degrees),
            cdMatrix=afwGeom.makeCdMatrix(scale=0.5*geom.arcseconds)
        )
        # Make a mock photoCalib
        self.mockPhotoCalib = afwImage.PhotoCalib()

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
        # For this test, we don't care that this is not a real detector column.
        config.measure.columns = {"detector": "id"}
        t = DetectorTableMeasurementTask(config)
        outputs = t.run(catalog=catalog)
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testTractMeasurementTaskNoCalibs(self):
        """Test run method of TractMeasurementTask with calibs as None."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = TractMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        config.requireAstrometry = False  # We are passing in None, so can't require
        config.requirePhotometry = False  # We are passing in None, so can't require
        t = TractMeasurementTask(config)
        outputs = t.run(
            catalogs=[catalog, ],
            photoCalibs=[None, ],
            astromCalibs=[None, ],
            dataIds=[{'band': 'r'}, ],
        )
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testTractTableValueMeasurementTask(self):
        table = self.load_data('CatalogMeasurementBaseTask').asAstropy().to_pandas()
        config = TractTableValueMeasurementConfig(
            band_order=[''],
            format_column='{band}{column}',
            prefixes_column=[''],
            row=0,
        )
        config.action.column = 'parent'
        t = TractTableValueMeasurementTask(config=config)
        outputs = t.run(table=table, bands=[''], name_metric='parent')
        expected = 0 * u.Unit('')
        self.assertEqual(outputs.measurement[0].quantity, expected)

    def testTractMeasurementTask(self):
        """Test run method of TractMeasurementTask with mixed calib as None."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = TractMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        t = TractMeasurementTask(config)

        # Add four input catalogs, only one of which should get added because
        # we have left requireAstrometry and requirePhotometry as True.
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

        # Add four input catalogs, only two of which should get added because
        # we have set requireAstrometry to False but left requirePhotometry
        # as True.
        config.requireAstrometry = False
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 2*771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

        # Add four input catalogs, only two of which should get added because
        # we have set requirePhotometry to False but left requireAstrometry
        # as True.
        config.requireAstrometry = True
        config.requirePhotometry = False
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 2*771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testVisitMeasurementTaskNoCalibs(self):
        """Test run method of VisitMeasurementTask with calibs as None."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = VisitMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        config.requireAstrometry = False  # We are passing in None, so can't require
        config.requirePhotometry = False  # We are passing in None, so can't require
        t = VisitMeasurementTask(config)
        outputs = t.run(
            catalogs=[catalog, ],
            photoCalibs=[None, ],
            astromCalibs=[None, ],
            dataIds=[{'band': 'r'}, ],
        )
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

    def testVisitMeasurementTask(self):
        """Test run method of VisitMeasurementTask with mixed calib as None."""
        catalog = self.load_data('CatalogMeasurementBaseTask')
        config = VisitMeasurementConfig()
        config.measure.retarget(NumSourcesMergeTask)
        t = VisitMeasurementTask(config)

        # Add four input catalogs, only one of which should get added because
        # we have left requireAstrometry and requirePhotometry as True.
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

        # Add four input catalogs, only two of which should get added because
        # we have set requireAstrometry to False but left requirePhotometry
        # as True.
        config.requireAstrometry = False
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 2*771 * u.count
        self.assertEqual(outputs.measurement.quantity, expected)

        # Add four input catalogs, only two of which should get added because
        # we have set requirePhotometry to False but left requireAstrometry
        # as True.
        config.requireAstrometry = True
        config.requirePhotometry = False
        outputs = t.run(
            catalogs=[catalog]*4,
            photoCalibs=[None, self.mockPhotoCalib, None, self.mockPhotoCalib],
            astromCalibs=[None, self.mockWcs, self.mockWcs, None],
            dataIds=[{'band': 'r'}]*4,
        )
        expected = 2*771 * u.count
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
