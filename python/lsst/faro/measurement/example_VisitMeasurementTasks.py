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
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement

__all__ = ("StarFracTask",)


class StarFracTask(Task):
    ConfigClass = Config
    _DefaultName = "starFracTask"

    def run(self, metricName, catalog, vIds):
        self.log.info("Measuring %s", metricName)
        if not catalog.isContiguous():
            catalog = catalog.copy(deep=True)
        extended = catalog["base_ClassificationExtendedness_value"]
        good_extended = extended[~catalog["base_ClassificationExtendedness_flag"]]
        n_gals = sum(good_extended)
        frac = 100 * (len(good_extended) - n_gals) / len(good_extended)
        meas = Measurement("starFrac", frac * u.percent)
        return Struct(measurement=meas)
