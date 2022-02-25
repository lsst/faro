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
    "ForcedSourceTableMeasurementConnections",
    "ForcedSourceTableMeasurementConfig",
    "ForcedSourceTableMeasurementTask",
    "ForcedSourceMultiBandTableMeasurementConnections",
    "ForcedSourceMultiBandTableMeasurementConfig",
    "ForcedSourceMultiBandTableMeasurementTask",
)


class ForcedSourceTableMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "skymap", "band"),
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Forced source table in parquet format, per tract",
        dimensions=("tract", "skymap"),
        storageClass="DataFrame",
        name="forcedSourceTable_tract",
        deferLoad=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class ForcedSourceTableMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=ForcedSourceTableMeasurementConnections
):
    """Configuration for ForcedSourceTableMeasurementTask."""


class ForcedSourceTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for per-band science performance metrics measured on multi-visit forced source catalogs."""

    ConfigClass = ForcedSourceTableMeasurementConfig
    _DefaultName = "forcedSourceTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        kwargs = {"currentBands": butlerQC.quantum.dataId['band']}

        columns = list(self.config.measure.columns.values())
        for column in self.config.measure.columnsBand.values():
            columns.append(kwargs["currentBands"] + "_" + column)
        
        columnsWithSelectors = self._getTableColumnsSelectors(columns, None)
        #columnsWithSelectors = self._getTableColumnsSelectors(columns, kwargs["currentBands"])
        
        tmp_catalog = inputs["catalog"].get(parameters={"columns": columnsWithSelectors})
        
        # Extract only the entries from the band of interest:
        kwargs["catalog"] = tmp_catalog.loc[tmp_catalog.band == kwargs["currentBands"],:]

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )


class ForcedSourceMultiBandTableMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "skymap"),
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Forced source table in parquet format, per tract",
        dimensions=("tract", "skymap"),
        storageClass="DataFrame",
        name="forcedSourceTable_tract",
        deferLoad=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class ForcedSourceMultiBandTableMeasurementConfig(
    ForcedSourceTableMeasurementConfig,
    pipelineConnections=ForcedSourceMultiBandTableMeasurementConnections
):
    """Configuration for ForcedSourceMultiBandTableMeasurementTask."""

    bands = pexConfig.ListField(
        doc="Bands for band-specific column loading from ForcedSourceTable_tract.",
        dtype=str,
        default=["g", "r", "i", "z", "y"],
    )


class ForcedSourceMultiBandTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for multi-band science performance metrics measured on
       multi-visit forced source catalogs."""

    ConfigClass = ForcedSourceMultiBandTableMeasurementConfig
    _DefaultName = "forcedSourceMultiBandTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        kwargs = {"currentBands": self.config.bands.list()}

        columns = list(self.config.measure.columns.values())
        for band in kwargs["currentBands"]:
            for column in self.config.measure.columnsBand.values():
                columns.append(band + "_" + column)

        columnsWithSelectors = self._getTableColumnsSelectors(columns, kwargs["currentBands"])
        tmp_catalog = inputs["catalog"].get(parameters={"columns": columnsWithSelectors})
        # Extract only the entries from the band of interest:
        kwargs["catalog"] = tmp_catalog[tmp_catalog.band.isin(kwargs["currentBands"])]

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
