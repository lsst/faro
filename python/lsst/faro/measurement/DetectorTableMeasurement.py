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
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import (CatalogMeasurementBaseConnections,
                                                   CatalogMeasurementBaseConfig,
                                                   CatalogMeasurementBaseTask)

__all__ = ("DetectorTableMeasurementConfig", "DetectorTableMeasurementTask")


class DetectorTableMeasurementConnections(CatalogMeasurementBaseConnections,
                                          dimensions=("instrument", "visit", "detector", "band"),
                                          defaultTemplates={"refDataset": ""}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source table in parquet format, per visit",
        dimensions=("instrument", "visit", "band"),
        storageClass="DataFrame",
        name="sourceTable_visit",
        deferLoad=True
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}"
    )


class DetectorTableMeasurementConfig(CatalogMeasurementBaseConfig,
                                     pipelineConnections=DetectorTableMeasurementConnections):
    """Configuration for DetectorTableMeasurementTask."""

    columns = pexConfig.ListField(doc="Columns from sourceTable_visit to load.",
                                  dtype=str, default=['coord_ra', 'coord_dec', 'detector'])

    def validate(self):
        super().validate()
        if 'detector' not in self.columns:
            msg = "The column `detector` must be appear in the list of columns."
            raise pexConfig.FieldValidationError(DetectorTableMeasurementConfig.columns, self, msg)


class DetectorTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for science performance metrics measured on single-detector source catalogs."""

    ConfigClass = DetectorTableMeasurementConfig
    _DefaultName = "detectorTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        catalog = inputs['catalog'].get(parameters={'columns': self.config.columns})
        selection = (catalog['detector'] == butlerQC.quantum.dataId['detector'])
        catalog = catalog[selection]

        kwargs = {}
        if self.config.connections.refDataset != '':
            refCats = inputs.pop('refCat')
            filterList = [butlerQC.quantum.dataId.records['physical_filter'].name]
            # Time at the start of the visit
            epoch = butlerQC.quantum.dataId.records['visit'].timespan.begin
            refCat, refCatCorrected = self._getReferenceCatalog(butlerQC,
                                                                [ref.datasetRef.dataId
                                                                 for ref in inputRefs.refCat],
                                                                refCats,
                                                                filterList,
                                                                epoch)
            kwargs['refCat'] = refCat
            kwargs['refCatCorrected'] = refCatCorrected

        outputs = self.run(catalog, **kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)
