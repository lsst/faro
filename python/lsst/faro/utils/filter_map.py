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

import os
import fnmatch

from lsst.utils import getPackageDir
from lsst.pex.config import Config, DictField, ConfigDictField

__all__ = ("FilterMapDict", "FilterMap")


class FilterMapNotFoundError(LookupError):
    """Exception class indicating we couldn't find a filter map
    """
    pass

class FilterMapDict(Config):
    """A mapping of band to physical filter label.
    """
    data = DictField(
        doc="Mapping of band to physical filter label",
        keytype=str,
        itemtype=str,
        default={},
    )


class FilterMap(Config):
    """Doc string."""
    data = ConfigDictField(
        doc="Mapping of band to physical filter label.",
        keytype=str, 
        itemtype=FilterMapDict,
        default={},
    )

    def __init__(self, filename=None):
        if filename is None:
            filename = os.path.join(getPackageDir('faro'), 'config', 'filterMap.py')
        self.load(filename)

    def getFilters(self, instName, bands, doRaise=True):
        """Doc string."""
        try:
            trueInstName = None
            filterDictConfig = self.data.get(instName)
            if filterDictConfig is None:
                # try glob expression
                matchList = [libInstNameGlob for libInstNameGlob in self.data
                             if fnmatch.fnmatch(instName, libInstNameGlob)]
                if len(matchList) == 1:
                    trueInstName = matchList[0]
                    filterDictConfig = self.data[trueInstName]
                elif len(matchList) > 1:
                    raise FilterMapNotFoundError(
                        "Multiple library globs match instName %r: %s" % (instName, matchList))
                else:
                    raise FilterMapNotFoundError(
                        "No filer map dict found with instName %r" % instName)

            filterDict = filterDictConfig.data

            filters = []
            for band in bands:
                if band not in filterDict:
                    errMsg = "No filter found for band %r with instName %r" % (
                        band, instName)
                    if trueInstName is not None:
                        errMsg += " = catalog %r" % (trueInstName,)
                    raise ColortermNotFoundError(errMsg)

                filters.append(filterDict[band])
            return filters
        except FilterMapNotFoundError:
            if doRaise:
                raise
            else:
                return bands

