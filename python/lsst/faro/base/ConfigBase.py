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


from lsst.pex.config import Config, DictField, Field
from lsst.pipe.tasks.configurableActions import ConfigurableActionStructField


class MeasurementTaskConfig(Config):

    selectorActions = ConfigurableActionStructField(
        doc="Which selectors to use to narrow down the data (independent of band).",
        default={},
    )
    columns = DictField(
        doc="""Columns required for metric calculation. These are full column names, encompassing all columns
        in a SourceTable and columns that do not change name with band in an ObjectTable. If per-band columns
        are required, use `columnsBand` instead.""",
        keytype=str,
        itemtype=str,
        default={}
    )
    columnsBand = DictField(
        doc="""Column suffixes used to identify the required columns for metric calculation. The band name
        will be prepended to this stub to select the columns of interest. These values are used to select
        flux columns in an objectTable.""",
        keytype=str,
        itemtype=str,
        default={}
    )
    shelveName = Field(
        doc="""Name of shelve file to persist in-memory objects sent as input to the metric
        measurement run method. Used for testing, development, and debug work.""",
        dtype=str,
        default="",
    )

    def _getColumnName(self, keyName, band=None):
        """Return column name corresponding to keyName if keyName is in columns or columnsBand"""
        columnsKeysSet = set(self.columns.keys())
        columnsBandKeysSet = set(self.columnsBand.keys())
        allKeys = set.union(columnsKeysSet, columnsBandKeysSet)

        assert (columnsKeysSet.isdisjoint(columnsBandKeysSet)), "duplicate key exists"
        assert (keyName in allKeys), "Key is not defined in columns"

        if keyName in columnsKeysSet:
            columnName = self.columns[keyName]
        elif keyName in columnsBandKeysSet:
            columnName = band + '_' + self.columnsBand[keyName]

        return columnName
