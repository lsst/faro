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


from lsst.pex.config import Config
from lsst.pipe.tasks.configurableActions import ConfigurableActionStructField


class MeasurementTaskConfig(Config):

    selectorActions = ConfigurableActionStructField(
        doc="Which selectors to use to narrow down the data (independent of band).",
        default={},
    )

    def _getColumnName(self,keyName,band=None):
        """Return column name corresponding to keyName if keyName is in columns or columnsBand"""
        #check no duplicate keys 
        columnsKeysList=list(self.columns.keys())
        columnsBandKeysList=list(self.columnsBand.keys())
        allKeys=columnsKeysList + columnsBandKeysList
        
        assert (len(allKeys) == len(set(allKeys))), "duplicate key exists" #no duplicate keys
        assert (keyName in allKeys), "Key is not defined" #key exists in one of the dicts
        
        if keyName in columnsKeysList:
            columnName = self.columns[keyName]
        elif keyName in columnsBandKeysList:
            columnName = band + '_' + self.columnsBand[keyName]    

        return columnName