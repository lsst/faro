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

from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig

from .BaseSubTasks import NumpySummaryTask

__all__ = (
    "CatalogSummaryBaseConnections",
    "CatalogSummaryBaseConfig",
    "CatalogSummaryBaseTask",
)


# Dimensions of the Connections class define the iterations of runQuantum
class CatalogSummaryBaseConnections(
    MetricConnections,
    defaultTemplates={"agg_name": None},
    dimensions=("band", "tract", "instrument", "skymap"),
):
    # Make this an LSST verify Measurement
    measurement = pipeBase.connectionTypes.Output(
        doc="{agg_name} {package}_{metric}.",
        dimensions=("instrument", "tract", "band"),
        storageClass="MetricValue",
        name="metricvalue_{agg_name}_{package}_{metric}",
    )


class CatalogSummaryBaseConfig(
    MetricConfig, pipelineConnections=CatalogSummaryBaseConnections
):
    agg = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumpySummaryTask,
        doc="Numpy aggregation task",
    )


class CatalogSummaryBaseTask(MetricTask):

    ConfigClass = CatalogSummaryBaseConfig
    _DefaultName = "catalogSummaryBaseTask"

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask("agg")

    def run(self, measurements):
        return self.agg.run(
            measurements,
            self.config.connections.agg_name,
            self.config.connections.package,
            self.config.connections.metric,
        )
