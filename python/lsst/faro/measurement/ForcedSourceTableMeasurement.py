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
from lsst.faro.utils.filter_map import FilterMap

__all__ = (
    "ForcedSourceTableMeasurementConnections",
    "ForcedSourceTableMeasurementConfig",
    "ForcedSourceTableMeasurementTask",
    # "ForcedSourceMultiBandTableMeasurementConnections",
    # "ForcedSourceMultiBandTableMeasurementConfig",
    # "ForcedSourceMultiBandTableMeasurementTask",
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

    columns = pexConfig.ListField(
        doc="Band-independent columns from forcedSourceTable_tract to load.",
        dtype=str,
        default=["coord_ra", "coord_dec", "band", "detect_isPrimary", "psfFlux", "psfFluxErr"],
    )

    # columnsBand = pexConfig.ListField(
        # doc="Band-specific columns from forcedSourceTable_tract to load.",
        # dtype=str,
        # default=["psfFlux", "psfFluxErr"],
    # )

    instrument = pexConfig.Field(
        doc="Instrument.",
        dtype=str,
        default='hsc',
    )


class ForcedSourceTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for per-band science performance metrics measured on multi-visit forced source catalogs."""

    ConfigClass = ForcedSourceTableMeasurementConfig
    _DefaultName = "forcedSourceTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        kwargs = {"band": butlerQC.quantum.dataId['band']}

        columns = self.config.columns.list()
        # for column in self.config.columnsBand:
            # columns.append(kwargs["band"] + column)
        tmp_catalog = inputs["catalog"].get(parameters={"columns": columns})
        # Extract only the entries from the band of interest:
        kwargs["catalog"] = tmp_catalog[tmp_catalog.band == kwargs["band"]]

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
