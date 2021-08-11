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

__all__ = (
    "PatchMeasurementConnections",
    "PatchMeasurementConfig",
    "PatchMeasurementTask",
)


class PatchMeasurementConnections(
    CatalogMeasurementBaseConnections, dimensions=("tract", "patch", "skymap", "band")
):

    cat = pipeBase.connectionTypes.Input(
        doc="Object catalog.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="SourceCatalog",
        name="deepCoadd_forced_src",
    )

    measurement = pipeBase.connectionTypes.Output(
        doc="Per-patch measurement.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class PatchMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=PatchMeasurementConnections
):
    pass


class PatchMeasurementTask(CatalogMeasurementBaseTask):

    ConfigClass = PatchMeasurementConfig
    _DefaultName = "patchMeasurementTask"

    def run(self, cat, vIds):
        return self.measure.run(cat, self.config.connections.metric, vIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs["vIds"] = inputRefs.cat.dataId
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
