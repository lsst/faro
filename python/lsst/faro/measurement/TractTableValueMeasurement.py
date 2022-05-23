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

import astropy.units as u

import lsst.pex.config as pexConfig
import lsst.pipe.base.connectionTypes as cT
from lsst.pipe.base import Struct
from lsst.pipe.tasks.diff_matched_tract_catalog import SourceType
from lsst.pipe.tasks.configurableActions import ConfigurableActionField
from lsst.pipe.tasks.dataFrameActions import SingleColumnAction
from lsst.verify import Measurement
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections, MetricComputationError

__all__ = (
    "TractTableValueMeasurementConnections",
    "TractTableValueMeasurementConfig",
    "TractTableValueMeasurementTask",
)


class TractTableValueMeasurementConnections(
    MetricConnections,
    defaultTemplates={"package": None, "metric": None, "name_table": None},
    dimensions=("tract", "skymap"),
):
    columns = cT.Input(
        doc="Table columns to read",
        name="{name_table}.columns",
        storageClass="DataFrameIndex",
        dimensions=("tract", "skymap"),
    )
    measurement = cT.Output(
        name="metricvalue_{package}_{metric}",
        doc="The metric value computed by this task.",
        storageClass="MetricValue",
        dimensions=("tract", "skymap", "band"),
        multiple=True,
    )
    table = cT.Input(
        doc="Table to read value from",
        name="{name_table}",
        storageClass="DataFrame",
        dimensions=("tract", "skymap"),
        deferLoad=True,
    )


class TractTableValueMeasurementConfig(
    MetricConfig, pipelineConnections=TractTableValueMeasurementConnections
):
    """Configuration for TractTableValueMeasurementTask."""
    action = ConfigurableActionField(
        doc="Action to compute the value with",
        default=SingleColumnAction,
    )
    band_order = pexConfig.ListField(
        dtype=str,
        doc="Standard (usually wavelength-based) ordering for possible bands"
            " to determine standard colors",
        default=('u', 'g', 'r', 'i', 'z', 'y'),
    )
    format_column = pexConfig.Field(
        dtype=str,
        doc="Format of the full column names including the band",
        default="{band}_{column}",
    )
    prefixes_column = pexConfig.ListField(
        dtype=str,
        doc="Column name prefixes to ignore when applying special formatting rules",
        default=[f'{x.value.label}_' for x in SourceType],
    )
    row = pexConfig.Field(
        dtype=int,
        doc="Index of the row to retrieve the value from",
        optional=False,
    )
    unit = pexConfig.Field(
        dtype=str,
        doc="The astropy unit of the metric value",
        default='',
    )

    def _format_column(self, band: str, column: str):
        prefix = ''
        for prefix_column in self.prefixes_column:
            if column.startswith(prefix_column):
                prefix = prefix_column
                column = column[len(prefix):]
                break
        if column.startswith('color_'):
            column = f'color_{self.band_order[self.band_order.index(band)-  1]}_m_{band}_{band}_{column[6:]}'
        if column.startswith('flux_'):
            column = f'flux_{band}_{column[5:]}'
        elif column.startswith('mag_'):
            column = f'mag_{band}_{column[4:]}'
        return self.format_column.format(band=band, column=f'{prefix}{column}')


class TractTableValueMeasurementTask(MetricTask):
    """Measure a metric from a single row and combination of columns in a table."""

    ConfigClass = TractTableValueMeasurementConfig
    _DefaultName = "TractTableValueMeasurementTask"

    def run(self, table, bands, name_metric):
        unit = u.Unit(self.config.unit)
        measurements = [None]*len(bands)
        columns = list(self.config.action.columns)
        for idx, band in enumerate(bands):
            row = table.iloc[[self.config.row]].rename(
                columns={self.config._format_column(band, column): column
                         for column in columns}
            )
            value = self.config.action(row).iloc[0]
            measurements[idx] = Measurement(name_metric, value*unit)
        return Struct(measurement=measurements)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        try:
            inputs = butlerQC.get(inputRefs)
            bands = [x.dataId['band'] for x in [y for y in outputRefs][0][1]]
            columns_base = list(self.config.action.columns)
            columns_in = []
            for band in bands:
                columns_in.extend(self.config._format_column(band, column)
                                  for column in columns_base)

            # If columns_in contains non-existent columns, the get call will fail
            outputs = self.run(
                table=inputs['table'].get(parameters={'columns': columns_in}),
                bands=bands,
                name_metric=self.config.connections.metric,
            )
            butlerQC.put(outputs, outputRefs)
        except MetricComputationError:
            self.log.error(
                "Measurement of %r failed on %s->%s",
                self, inputRefs, outputRefs, exc_info=True)
