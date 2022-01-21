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


from lsst.pex.config import Config, Field
from lsst.pex.config.callStack import getStackFrame
from lsst.pipe.tasks.configurableActions import ConfigurableActionStructField


class ColumnField(Field):
    """A configuration field (`lsst.pex.config.Field` subclass) that indicates
    a band-independent column name."""

    def __init__(self, doc, default=None, check=None, optional=False, deprecated=None):

        source = getStackFrame()
        self._setup(
            doc=doc,
            dtype=str,
            default=default,
            check=check,
            optional=optional,
            source=source,
            deprecated=deprecated,
        )


class ColumnBandField(Field):
    """A configuration field (`lsst.pex.config.Field` subclass) that indicates
    a by-band column name."""

    def __init__(self, doc, default=None, check=None, optional=False, deprecated=None):

        source = getStackFrame()
        self._setup(
            doc=doc,
            dtype=str,
            default=default,
            check=check,
            optional=optional,
            source=source,
            deprecated=deprecated,
        )


class MeasurementTaskConfig(Config):

    selectorActions = ConfigurableActionStructField(
        doc="Which selectors to use to narrow down the data (independent of band).",
        default={},
    )

    def columns(self):
        """Return list of band-independent column names that are set in 
        configuration."""

        columns = []
        for name, value in self.items():
            if isinstance(self._fields[name], ColumnField):
                self
                columns.append(value)

        return columns

    def columnsBand(self):
        """Return list of by-band column names that are set in configuration.
        """

        columns = []
        for name, value in self.items():
            if isinstance(self._fields[name], ColumnBandField):
                self
                columns.append(value)

        return columns