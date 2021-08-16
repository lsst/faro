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

import traceback

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricComputationError

from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)

__all__ = (
    "PatchMatchedMeasurementConnections",
    "PatchMatchedMeasurementConfig",
    "PatchMatchedMeasurementTask",
    "TractMatchedMeasurementConnections",
    "TractMatchedMeasurementConfig",
    "TractMatchedMeasurementTask",
    "PatchMatchedMultiBandMeasurementConnections",
    "PatchMatchedMultiBandMeasurementConfig",
    "PatchMatchedMultiBandMeasurementTask",
)

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class PatchMatchedMeasurementConnections(
    CatalogMeasurementBaseConnections, dimensions=("tract", "patch", "band", "instrument", "skymap")
):
    cat = pipeBase.connectionTypes.Input(
        doc="Input matched catalog.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatch",
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class PatchMatchedMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=PatchMatchedMeasurementConnections
):
    pass


class PatchMatchedMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = PatchMatchedMeasurementConfig
    _DefaultName = "patchMatchedMeasurementTask"


class TractMatchedMeasurementConnections(
    PatchMatchedMeasurementConnections,
    dimensions=("tract", "instrument", "band", "skymap"),
):
    cat = pipeBase.connectionTypes.Input(
        doc="Input matched catalog.",
        dimensions=("tract", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogTract",
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class TractMatchedMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=TractMatchedMeasurementConnections
):
    pass


class TractMatchedMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = TractMatchedMeasurementConfig
    _DefaultName = "tractMatchedMeasurementTask"


class PatchMatchedMultiBandMeasurementConnections(
    CatalogMeasurementBaseConnections, dimensions=("tract", "patch", "band", "instrument", "skymap")
):
    cat = pipeBase.connectionTypes.Input(
        doc="Input matched catalog.",
        dimensions=("tract", "patch", "instrument"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatchMultiBand",
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )


class PatchMatchedMultiBandMeasurementConfig(
    CatalogMeasurementBaseConfig,
    pipelineConnections=PatchMatchedMultiBandMeasurementConnections,
):
    pass


class PatchMatchedMultiBandMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = PatchMatchedMultiBandMeasurementConfig
    _DefaultName = "patchMatchedMultiBandMeasurementTask"

    def run(self, cat, in_id, out_id):
        return self.measure.run(cat, self.config.connections.metric, in_id, out_id)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        """Do Butler I/O to provide in-memory objects for run.
        This specialization of runQuantum performs error-handling specific to
        MetricTasks. Most or all of this functionality may be moved to
        activators in the future.
        """
        try:
            in_id = butlerQC.registry.expandDataId(inputRefs.cat.dataId)
            out_id = butlerQC.registry.expandDataId(outputRefs.measurement.dataId)
            inputs = butlerQC.get(inputRefs)
            inputs["in_id"] = in_id
            inputs["out_id"] = out_id
            outputs = self.run(**inputs)
            if outputs.measurement is not None:
                butlerQC.put(outputs, outputRefs)
            else:
                self.log.debug(
                    "Skipping measurement of {!r} on {} " "as not applicable.",
                    self,
                    inputRefs,
                )
        except MetricComputationError as e:
            self.log.error(
                "Measurement of {!r} failed on {}->{}\n{}\n,%s",
                self,
                inputRefs,
                outputRefs,
                traceback.format_exc(),
                e.msg,
            )
