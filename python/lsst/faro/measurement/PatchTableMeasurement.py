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
    "PatchTableMeasurementConnections",
    "PatchTableMeasurementConfig",
    "PatchTableMeasurementTask",
    "PatchMultiBandTableMeasurementConnections",
    "PatchMultiBandTableMeasurementConfig",
    "PatchMultiBandTableMeasurementTask",
)


class PatchTableMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "patch", "skymap", "band"),
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Object table in parquet format, per tract.",
        dimensions=("tract", "skymap"),
        storageClass="DataFrame",
        name="objectTable_tract",
        deferLoad=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class PatchTableMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=PatchTableMeasurementConnections
):
    """Configuration for PatchTableMeasurementTask."""

    columns = pexConfig.ListField(
        doc="Band-independent columns from objectTable_tract to load.",
        dtype=str,
        default=["coord_ra", "coord_dec", "detect_isPrimary", "patch"],
    )

    columnsBand = pexConfig.ListField(
        doc="Band-specific columns from objectTable_tract to load.",
        dtype=str,
        default=["psfFlux", "psfFluxErr"],
    )

    instrument = pexConfig.Field(
        doc="Instrument.",
        dtype=str,
        default='hsc',
    )


class PatchTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for per-band science performance metrics measured on single-tract object catalogs."""

    ConfigClass = PatchTableMeasurementConfig
    _DefaultName = "tractTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        kwargs = {"band": butlerQC.quantum.dataId['band']}

        columns = self.config.columns.list()
        for column in self.config.columnsBand:
            columns.append(kwargs["band"] + column)
        catalog = inputs["catalog"].get(parameters={"columns": columns})
        selection = (catalog["patch"] == butlerQC.quantum.dataId["patch"])
        kwargs["catalog"] = catalog[selection]

        if self.config.connections.refDataset != "":
            refCats = inputs.pop("refCat")
            filter_map = FilterMap()
            filterList = filter_map.getFilters(self.config.instrument,
                                               [kwargs["band"]])

            # TODO: add capability to select the reference epoch
            epoch = None
            refCat, refCatCorrected = self._getReferenceCatalog(
                butlerQC,
                [ref.datasetRef.dataId for ref in inputRefs.refCat],
                refCats,
                filterList,
                epoch,
            )
            kwargs["refCat"] = refCat
            kwargs["refCatCorrected"] = refCatCorrected

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )


class PatchMultiBandTableMeasurementConnections(
    PatchTableMeasurementConnections,
    dimensions=("tract", "patch", "skymap"),
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Object table in parquet format, per tract.",
        dimensions=("tract", "skymap"),
        storageClass="DataFrame",
        name="objectTable_tract",
        deferLoad=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "patch", "skymap"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class PatchMultiBandTableMeasurementConfig(
    PatchTableMeasurementConfig,
    pipelineConnections=PatchMultiBandTableMeasurementConnections,
):
    """Configuration for PatchMultiBandTableMeasurementTask."""

    bands = pexConfig.ListField(
        doc="Bands for band-specific column loading from objectTable_tract.",
        dtype=str,
        default=["g", "r", "i", "z", "y"],
    )


class PatchMultiBandTableMeasurementTask(PatchTableMeasurementTask):

    """Base class for science performance metrics measured on single-tract source catalogs, multi-band."""

    ConfigClass = PatchMultiBandTableMeasurementConfig
    _DefaultName = "tractMultiBandTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)

        kwargs = {"bands": self.config.bands.list()}

        columns = self.config.columns.list()
        for band in self.config.bands:
            for column in self.config.columnsBand:
                columns.append(band + column)
        catalog = inputs["catalog"].get(parameters={"columns": columns})
        selection = (catalog["patch"] == butlerQC.quantum.dataId["patch"])
        kwargs["catalog"] = catalog[selection]

        if self.config.connections.refDataset != "":
            refCats = inputs.pop("refCat")
            filter_map = FilterMap()
            filterList = filter_map.getFilters(self.config.instrument,
                                               self.config.bands)

            # TODO: add capability to select the reference epoch
            epoch = None
            refCat, refCatCorrected = self._getReferenceCatalog(
                butlerQC,
                [ref.datasetRef.dataId for ref in inputRefs.refCat],
                refCats,
                filterList,
                epoch,
            )
            kwargs["refCat"] = refCat
            kwargs["refCatCorrected"] = refCatCorrected

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )