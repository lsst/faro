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

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig
import lsst.geom
from lsst.utils import getPackageDir

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask
from lsst.pipe.tasks.loadReferenceCatalog import LoadReferenceCatalogConfig, LoadReferenceCatalogTask

__all__ = ("DetectorTableMeasurementConfig", "DetectorTableMeasurementTask")


class DetectorTableMeasurementConnections(MetricConnections,
                                          dimensions=("instrument", "visit", "detector", "band"),
                                          defaultTemplates={"refDataset": ""}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog for visit.",
        dimensions=("instrument", "visit", "band"),
        storageClass="DataFrame",
        name="sourceTable_visit",
        deferLoad=True
    )

    refcat = pipeBase.connectionTypes.PrerequisiteInput(
        doc="Reference catalog.",
        name="{refDataset}",
        storageClass="SimpleCatalog",
        dimensions=("skypix",),
        deferLoad=True,
        multiple=True
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}"
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.connections.refDataset == '':
            self.prerequisiteInputs.remove("refcat")


class DetectorTableMeasurementConfig(CatalogMeasurementBaseTaskConfig,
                                     pipelineConnections=DetectorTableMeasurementConnections):
    columns = pexConfig.ListField(doc="Columns from sourceTable_visit to load.",
                                  dtype=str, default=['coord_ra', 'coord_dec', 'detector'])

    def validate(self):
        super().validate()
        if 'detector' not in self.columns:
            msg = "The column `detector` must be appear in the list of columns."
            raise pexConfig.FieldValidationError(DetectorTableMeasurementConfig.columns, self, msg)


class DetectorTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for science performance metrics measured on single-dector source catalogs."""
    ConfigClass = DetectorTableMeasurementConfig
    _DefaultName = "detectorTableMeasurementTask"

    def run(self, catalog, refcat, refcatCalib):
        return self.measure.run(self.config.connections.metric, catalog,
                                refcat=refcat, refcatCalib=refcatCalib)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        catalog = inputs['catalog'].get(parameters={'columns': self.config.columns})
        selection = (catalog['detector'] == butlerQC.quantum.dataId['detector'])
        catalog = catalog[selection]

        if self.config.connections.refDataset != '':
            config = LoadReferenceCatalogConfig()
            config.refObjLoader.ref_dataset_name = self.config.connections.refDataset

            # These default configurations for reference catalogs should go
            # elsewhere in a common utility.
            if self.config.connections.refDataset == 'gaia_dr2_20200414':
                # Apply proper motions for Gaia catalog
                config.refObjLoader.requireProperMotion = True
                config.refObjLoader.anyFilterMapsToThis = 'phot_g_mean'
                config.doApplyColorTerms = False
            elif self.config.connections.refDataset == 'ps1_pv3_3pi_20170110':
                # Apply color terms for PS1 catalog
                config.refObjLoader.load(os.path.join(getPackageDir('obs_subaru'),
                                                      'config',
                                                      'filterMap.py'))
                config.colorterms.load(os.path.join(getPackageDir('obs_subaru'),
                                                    'config',
                                                    'colorterms.py'))

            center = lsst.geom.SpherePoint(butlerQC.quantum.dataId.region.getBoundingCircle().getCenter())
            radius = butlerQC.quantum.dataId.region.getBoundingCircle().getOpeningAngle()

            epoch = butlerQC.quantum.dataId.records['visit'].timespan.begin

            loaderTask = LoadReferenceCatalogTask(
                config=config,
                dataIds=[ref.datasetRef.dataId
                         for ref in inputRefs.refcat],
                refCats=inputs.pop('refcat'))

            # Catalog with proper motion and color terms applied
            refcatCalib = loaderTask.getSkyCircleCatalog(
                center,
                radius,
                [butlerQC.quantum.dataId.records['physical_filter'].name],
                epoch=epoch)

            # Next get unformatted catalog w/ all columns
            skyCircle = loaderTask.refObjLoader.loadSkyCircle(center, radius,
                                                              loaderTask._referenceFilter,
                                                              epoch=epoch)
            if not skyCircle.refCat.isContiguous():
                refcat = skyCircle.refCat.copy(deep=True)
            else:
                refcat = skyCircle.refCat

        outputs = self.run(catalog, refcat, refcatCalib)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)
