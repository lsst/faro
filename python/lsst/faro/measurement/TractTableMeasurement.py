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

from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)

__all__ = (
    "TractTableMeasurementConnections",
    "TractTableMeasurementConfig",
    "TractTableMeasurementTask",
    "TractTableMultiBandMeasurementConnections",
    "TractTableMultiBandMeasurementConfig",
    "TractTableMultiBandMeasurementTask",
)


class TractTableMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "skymap", "band"),
):

    catalogs = pipeBase.connectionTypes.Input(
        doc="Source table in parquet format, per tract",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="DataFrame",
        name="deepCoadd_forced_src",
        multiple=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )

    
class TractTableMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=TractTableMeasurementConnections
):
    """Configuration for TractTableMeasurementTask."""

    columns = pexConfig.ListField(
        doc="Columns from objectTable_tract to load.",
        dtype=str,
        default=["coord_ra", "coord_dec"],
    )

    
class TractTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for science performance metrics measured on single-tract source catalogs."""

    ConfigClass = TractTableMeasurementConfig
    _DefaultName = "tractTableMeasurementTask"

    def run(self, catalog):
        return self.measure.run(self.config.connections.metric, catalog)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        catalog = inputs["catalog"].get(parameters={"columns": self.config.columns})
        outputs = self.run(catalog)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )

            
class TractTableMultiBandMeasurementConnections(
    TractTableMeasurementConnections,
    dimensions=("tract", "skymap"),
):

    catalogs = pipeBase.connectionTypes.Input(
        doc="Object catalog.",
        dimensions=("tract", "skymap", "patch", "band"),
        storageClass="SourceCatalog",
        name="deepCoadd_forced_src",
        multiple=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class TractTableMultiBandMeasurementConfig(
    CatalogMeasurementBaseConfig,
    pipelineConnections=TractTableMultiBandMeasurementConnections,
):
    pass


class TractTableMultiBandMeasurementTask(TractTableMeasurementTask):

"""Base class for science performance metrics measured on single-tract source catalogs, multi-band."""

    ConfigClass = TractTableMultiBandMeasurementConfig
    _DefaultName = "tractTableMultiBandMeasurementTask"