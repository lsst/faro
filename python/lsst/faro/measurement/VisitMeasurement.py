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

__all__ = ("VisitMeasurementConfig", "VisitMeasurementTask")


class VisitMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("instrument", "visit", "band"),
    defaultTemplates={"photoCalibName": "calexp.photoCalib", "wcsName": "calexp.wcs"},
):

    catalogs = pipeBase.connectionTypes.Input(
        doc="Source catalogs.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src",
        multiple=True,
    )

    photoCalibs = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}",
        multiple=True,
    )

    astromCalibs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="Wcs",
        name="{wcsName}",
        multiple=True,
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-visit measurement.",
        dimensions=("instrument", "visit", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class VisitMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=VisitMeasurementConnections
):
    measure = pexConfig.ConfigurableField(
        # The (plain old) Task that actually measures the desired metric
        # Should be overridden in pipelines
        target=NumSourcesMergeTask,
        doc="Measure task",
    )


class VisitMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitMeasurementConfig
    _DefaultName = "visitMeasurementTask"

    def run(
            self,
            catalogs: List[SourceCatalog],
            photoCalibs: List[PhotoCalib],
            astromCalibs: List[SkyWcs],
            dataIds: List[dict],
    ):
        data = defaultdict(list)
        for catalog, photoCalib, astromCalib, dataId in zip(catalogs, photoCalibs, astromCalibs, dataIds):
            if self.config.requireAstrometry and astromCalib is None:
                self.log.info("requireAstrometry is True but astromCalib is None for %s.  Skipping...",
                              dataId)
                continue
            if self.config.requirePhotometry and photoCalib is None:
                self.log.info("requirePhotometry is True but photoCalib is None for %s.  Skipping...",
                              dataId)
                continue
            data[dataId["band"]].append(CalibratedCatalog(catalog, photoCalib, astromCalib))

        return self.measure.run(self.config.connections.metric, data)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs["dataIds"] = [c.dataId for c in inputRefs.catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
