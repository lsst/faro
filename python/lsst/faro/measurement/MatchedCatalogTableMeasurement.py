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

import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)

__all__ = (
    "PatchMatchedTableMeasurementConnections",
    "PatchMatchedTableMeasurementConfig",
    "PatchMatchedTableMeasurementTask",
    "TractMatchedTableMeasurementConnections",
    "TractMatchedTableMeasurementConfig",
    "TractMatchedTableMeasurementTask",
    "PatchMatchedMultiBandTableMeasurementConnections",
    "PatchMatchedMultiBandTableMeasurementConfig",
    "PatchMatchedMultiBandTableMeasurementTask",
)


class PatchMatchedMeasurementConnections(
    CatalogMeasurementBaseConnections, dimensions=("tract", "patch", "band", "instrument", "skymap")
):
    matchedCatalog = pipeBase.connectionTypes.Input(
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
