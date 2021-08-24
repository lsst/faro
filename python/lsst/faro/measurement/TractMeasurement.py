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

from lsst.afw.geom import SkyWcs
from lsst.afw.image import PhotoCalib
from lsst.afw.table import SourceCatalog
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

from lsst.faro.base.BaseSubTasks import NumSourcesMergeTask
from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)
from lsst.faro.utils.calibrated_catalog import CalibratedCatalog

from collections import defaultdict
from typing import List

__all__ = (
    "TractMeasurementConnections",
    "TractMeasurementConfig",
    "TractMeasurementTask",
    "TractMultiBandMeasurementConnections",
    "TractMultiBandMeasurementConfig",
    "TractMultiBandMeasurementTask",
)


class TractMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("tract", "skymap", "band"),
    defaultTemplates={
        "coaddName": "deepCoadd",
        "photoCalibName": "deepCoadd_calexp.photoCalib",
        "wcsName": "deepCoadd_calexp.wcs",
    },
):

    catalogs = pipeBase.connectionTypes.Input(
        doc="Object catalog.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="SourceCatalog",
        name="deepCoadd_forced_src",
        multiple=True,
    )

    photoCalibs = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}",
        multiple=True,
    )

    astromCalibs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="Wcs",
        name="{wcsName}",
        multiple=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class TractMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=TractMeasurementConnections
):
    measure = pexConfig.ConfigurableField(
        # The (plain old) Task that actually measures the desired metric
        # Should be overridden in pipelines
        target=NumSourcesMergeTask,
        doc="Measure task",
    )


class TractMeasurementTask(CatalogMeasurementBaseTask):

    ConfigClass = TractMeasurementConfig
    _DefaultName = "tractMeasurementTask"

    def run(
            self,
            catalogs: List[SourceCatalog],
            photoCalibs: List[PhotoCalib],
            astromCalibs: List[SkyWcs],
            dataIds,
    ):
        data = defaultdict(list)
        for catalog, photoCalib, astromCalib, dataId in zip(catalogs, photoCalibs, astromCalibs, dataIds):
            data[dataId['band']].append(CalibratedCatalog(catalog, photoCalib, astromCalib))

        return self.measure.run(self.config.connections.metric, data)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs["dataIds"] = [
            butlerQC.registry.expandDataId(cat.dataId) for cat in inputRefs.catalogs
        ]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )


class TractMultiBandMeasurementConnections(
    TractMeasurementConnections,
    dimensions=("tract", "skymap"),
    defaultTemplates={
        "coaddName": "deepCoadd",
        "photoCalibName": "deepCoadd_calexp.photoCalib",
    },
):

    catalogs = pipeBase.connectionTypes.Input(
        doc="Object catalog.",
        dimensions=("tract", "skymap", "patch", "band"),
        storageClass="SourceCatalog",
        name="deepCoadd_forced_src",
        multiple=True,
    )

    photoCalibs = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("tract", "skymap", "patch", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}",
        multiple=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-tract measurement.",
        dimensions=("tract", "skymap"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class TractMultiBandMeasurementConfig(
    CatalogMeasurementBaseConfig,
    pipelineConnections=TractMultiBandMeasurementConnections,
):
    pass


class TractMultiBandMeasurementTask(TractMeasurementTask):

    ConfigClass = TractMultiBandMeasurementConfig
    _DefaultName = "tractMultiBandMeasurementTask"
