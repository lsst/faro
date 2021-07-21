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

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig
import lsst.geom

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask
from lsst.pipe.tasks.loadReferenceCatalog import LoadReferenceCatalogTask

__all__ = ("DetectorTableMeasurementConfig", "DetectorTableMeasurementTask")


class DetectorTableMeasurementConnections(MetricConnections,
                                          dimensions=("instrument", "visit", "detector", "band"),
                                          defaultTemplates={"refDataset": "gaia_dr2_20200414"}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source table in parquet format, per visit",
        dimensions=("instrument", "visit", "band"),
        storageClass="DataFrame",
        name="sourceTable_visit",
        deferLoad=True
    )

    refcat = pipeBase.connectionTypes.PrerequisiteInput(
        doc="Reference catalog",
        name="{refDataset}",
        storageClass="SimpleCatalog",
        dimensions=("skypix",),
        deferLoad=True,
        multiple=True
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement",
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
    """Configuration for DetectorTableMeasurementTask."""

    columns = pexConfig.ListField(doc="Columns from sourceTable_visit to load.",
                                  dtype=str, default=['coord_ra', 'coord_dec', 'detector'])

    refObjLoader = pexConfig.ConfigurableField(
        target=LoadReferenceCatalogTask,
        doc="Reference object loader",
    )

    def setDefaults(self):
        self.refObjLoader.refObjLoader.ref_dataset_name = "gaia_dr2_20200414"
        self.refObjLoader.refObjLoader.requireProperMotion = True
        self.refObjLoader.refObjLoader.anyFilterMapsToThis = 'phot_g_mean'
        self.refObjLoader.doApplyColorTerms = False

    def validate(self):
        super().validate()
        if 'detector' not in self.columns:
            msg = "The column `detector` must be appear in the list of columns."
            raise pexConfig.FieldValidationError(DetectorTableMeasurementConfig.columns, self, msg)
        if self.connections.refDataset != self.refObjLoader.refObjLoader.ref_dataset_name:
            msg = "The reference datasets specified in connections and reference object loader must match."
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
            center = lsst.geom.SpherePoint(butlerQC.quantum.dataId.region.getBoundingCircle().getCenter())
            radius = butlerQC.quantum.dataId.region.getBoundingCircle().getOpeningAngle()

            epoch = butlerQC.quantum.dataId.records['visit'].timespan.begin

            loaderTask = LoadReferenceCatalogTask(
                config=self.config.refObjLoader,
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
