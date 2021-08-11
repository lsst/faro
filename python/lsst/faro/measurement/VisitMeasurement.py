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
    pass


class VisitMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitMeasurementConfig
    _DefaultName = "visitMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs["dataIds"] = [
            butlerQC.registry.expandDataId(c.dataId) for c in inputRefs.catalogs
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
