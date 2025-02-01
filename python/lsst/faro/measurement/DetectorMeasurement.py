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

from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)

__all__ = ("DetectorMeasurementConfig", "DetectorMeasurementTask")


class DetectorMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("instrument", "visit", "detector", "band"),
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="initial_stars_footprints_detector",
    )
    visitSummary = pipeBase.connectionTypes.Input(
        doc="Exposure catalog with WCS and PhotoCalib this detector+visit combination.",
        dimensions=("instrument", "visit"),
        storageClass="ExposureCatalog",
        name="finalVisitSummary",
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class DetectorMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=DetectorMeasurementConnections
):
    pass


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementConfig
    _DefaultName = "detectorMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        visitSummary = inputs.pop("visitSummary")
        detector = inputRefs.catalog.dataId["detector"]
        row = visitSummary.find(detector)
        inputs["photoCalib"] = row.getPhotoCalib()
        inputs["skyWcs"] = row.getSkyWcs()
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
