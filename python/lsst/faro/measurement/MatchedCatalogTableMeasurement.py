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
    "TractMatchedCatalogTableMeasurementConnections",
    "TractMatchedCatalogTableMeasurementConfig",
    "TractMatchedCatalogTableMeasurementTask",
    # "MatchedCatalogMultiBandTableMeasurementConnections",
    # "MatchedCatalogMultiBandTableMeasurementConfig",
    # "MatchedCatalogMultiBandTableMeasurementTask",
)


class TractMatchedCatalogTableMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "skymap", "band"),
    defaultTemplates={"individualSourceCatalogName": "isolated_star_sources"}
):

    individualSourceCatalog = pipeBase.connectionTypes.Input(
        doc='Catalog of individual sources for the isolated stars',
        name="{individualSourceCatalogName}",
        storageClass='DataFrame',
        dimensions=('instrument', 'tract', 'skymap'),
        deferLoad=True,
    )
    
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class TractMatchedCatalogTableMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=TractMatchedCatalogTableMeasurementConnections
):
    """Configuration for TractTableMeasurementTask."""

    instrument = pexConfig.Field(
        doc="Instrument.",
        dtype=str,
        default='hsc',
    )


class TractMatchedCatalogTableMeasurementTask(CatalogMeasurementBaseTask):
    """Base class for per-band science performance metrics measured on single-tract object catalogs."""

    ConfigClass = TractMatchedCatalogTableMeasurementConfig
    _DefaultName = "tractMatchedCatalogTableMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        kwargs = {"currentBands": butlerQC.quantum.dataId['band']}

        columns = list(self.config.measure.columns.values())
        for column in self.config.measure.columnsBand.values():
             columns.append(kwargs["currentBands"] + '_' + column)
        columnsWithSelectors = self._getTableColumnsSelectors(columns, kwargs["currentBands"])
        kwargs["catalog"] = inputs["individualSourceCatalog"].get(parameters={"columns": columnsWithSelectors})
        
        if self.config.connections.refDataset != "":
            refCats = inputs.pop("refCat")
            filter_map = FilterMap()
            filterList = filter_map.getFilters(self.config.instrument,
                                               [kwargs["currentBands"]])

            # TODO: add capability to select the reference epoch
            epoch = None
            refCatFrame = self._getReferenceCatalog(
                butlerQC,
                [ref.datasetRef.dataId for ref in inputRefs.refCat],
                refCats,
                filterList,
                epoch,
            )
            kwargs["refCatFrame"] = refCatFrame

        outputs = self.run(**kwargs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )


# class TractMultiBandTableMeasurementConnections(
#     TractTableMeasurementConnections,
#     dimensions=("tract", "skymap"),
# ):

#     catalog = pipeBase.connectionTypes.Input(
#         doc="Object table in parquet format, per tract.",
#         dimensions=("tract", "skymap"),
#         storageClass="DataFrame",
#         name="objectTable_tract",
#         deferLoad=True,
#     )

#     measurement = pipeBase.connectionTypes.Output(
#         doc="Per-tract measurement.",
#         dimensions=("tract", "skymap"),
#         storageClass="MetricValue",
#         name="metricvalue_{package}_{metric}",
#     )


# class TractMultiBandTableMeasurementConfig(
#     TractTableMeasurementConfig,
#     pipelineConnections=TractMultiBandTableMeasurementConnections,
# ):
#     """Configuration for TractMultiBandTableMeasurementTask."""

#     bands = pexConfig.ListField(
#         doc="Bands for band-specific column loading from objectTable_tract.",
#         dtype=str,
#         default=["g", "r", "i", "z", "y"],
#     )


# class TractMultiBandTableMeasurementTask(TractTableMeasurementTask):

#     """Base class for science performance metrics measured on single-tract object catalogs, multi-band."""

#     ConfigClass = TractMultiBandTableMeasurementConfig
#     _DefaultName = "tractMultiBandTableMeasurementTask"

#     def runQuantum(self, butlerQC, inputRefs, outputRefs):
#         inputs = butlerQC.get(inputRefs)

#         kwargs = {"currentBands": self.config.bands.list()}

#         columns = list(self.config.measure.columns.values())
#         for band in self.config.bands:
#             for column in self.config.measure.columnsBand.values():
#                 columns.append(band + "_" + column)
#         columnsWithSelectors = self._getTableColumnsSelectors(columns, kwargs["currentBands"])
#         kwargs["catalog"] = inputs["catalog"].get(parameters={"columns": columnsWithSelectors})

#         if self.config.connections.refDataset != "":
#             refCats = inputs.pop("refCat")
#             filter_map = FilterMap()
#             filterList = filter_map.getFilters(self.config.instrument,
#                                                self.config.bands)

#             # TODO: add capability to select the reference epoch
#             epoch = None
#             refCat = self._getReferenceCatalog(
#                 butlerQC,
#                 [ref.datasetRef.dataId for ref in inputRefs.refCat],
#                 refCats,
#                 filterList,
#                 epoch,
#             )
#             kwargs["refCat"] = refCat

#         outputs = self.run(**kwargs)
#         if outputs.measurement is not None:
#             butlerQC.put(outputs, outputRefs)
#         else:
#             self.log.debugf(
#                 "Skipping measurement of {!r} on {} " "as not applicable.",
#                 self,
#                 inputRefs,
#             )
