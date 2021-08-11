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

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = (
    "PatchMatchedSummaryConnections",
    "PatchMatchedSummaryConfig",
    "PatchMatchedSummaryTask",
    "TractMatchedSummaryConnections",
    "TractMatchedSummaryConfig",
    "TractMatchedSummaryTask",
)


# Dimensions of the Connections class define the iterations of runQuantum
class PatchMatchedSummaryConnections(CatalogSummaryBaseConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class PatchMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=PatchMatchedSummaryConnections
):
    pass


class PatchMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchMatchedSummaryConfig
    _DefaultName = "patchMatchedSummaryTask"


class TractMatchedSummaryConnections(CatalogSummaryBaseConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class TractMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=TractMatchedSummaryConnections
):
    pass


class TractMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = TractMatchedSummaryConfig
    _DefaultName = "tractMatchedSummaryTask"
