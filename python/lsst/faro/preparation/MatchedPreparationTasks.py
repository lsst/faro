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

from lsst.faro.base.MatchedCatalogBase import (
    MatchedBaseConnections,
    MatchedBaseConfig,
    MatchedBaseTask,
    MatchedTractBaseTask,
)

__all__ = (
    "PatchMatchedPreparationConnections",
    "PatchMatchedPreparationConfig",
    "PatchMatchedPreparationTask",
    "TractMatchedPreparationConnections",
    "TractMatchedPreparationConfig",
    "TractMatchedPreparationTask",
    "PatchMatchedMultiBandPreparationConnections",
    "PatchMatchedMultiBandPreparationConfig",
    "PatchMatchedMultiBandPreparationTask",
)


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchMatchedPreparationConnections(
    MatchedBaseConnections,
    dimensions=("tract", "patch", "band", "instrument", "skymap"),
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatch",
    )


class PatchMatchedPreparationConfig(
    MatchedBaseConfig, pipelineConnections=PatchMatchedPreparationConnections
):
    pass


class PatchMatchedPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedPreparationConfig
    _DefaultName = "patchMatchedPreparationTask"


class TractMatchedPreparationConnections(
    MatchedBaseConnections, dimensions=("tract", "band", "instrument", "skymap")
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogTract",
    )


class TractMatchedPreparationConfig(
    MatchedBaseConfig, pipelineConnections=TractMatchedPreparationConnections
):
    pass


class TractMatchedPreparationTask(MatchedTractBaseTask):

    ConfigClass = TractMatchedPreparationConfig
    _DefaultName = "tractMatchedPreparationTask"


class PatchMatchedMultiBandPreparationConnections(
    MatchedBaseConnections, dimensions=("tract", "patch", "instrument", "skymap")
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatchMultiBand",
    )


class PatchMatchedMultiBandPreparationConfig(
    MatchedBaseConfig,
    pipelineConnections=PatchMatchedMultiBandPreparationConnections,
):
    pass


class PatchMatchedMultiBandPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedMultiBandPreparationConfig
    _DefaultName = "patchMatchedMultiBandPreparationTask"
