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

import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement

__all__ = ("HistMedianTask",)


class HistMedianTask(Task):

    ConfigClass = Config
    _DefaultName = "histMedianTask"

    def run(self, measurements, agg_name, package, metric):
        self.log.info("Computing the %s of %s_%s values", agg_name, package, metric)
        values = measurements[0].extras["values"].quantity
        bins = measurements[0].extras["bins"].quantity
        for m in measurements[1:]:
            values += m.extras["values"].quantity
        c = np.cumsum(values)
        idx = np.searchsorted(c, c[-1] / 2.0)
        # This is the bin lower bound
        lower = bins[idx]
        # This is the bin upper bound
        upper = bins[idx + 1]

        # Linear interpolation of median value within the bin
        frac = (c[-1] / 2.0 - c[idx - 1]) / (c[idx] - c[idx - 1])
        interp = lower + (upper - lower) * frac

        return Struct(
            measurement=Measurement(
                f"metricvalue_{agg_name}_{package}_{metric}", interp
            )
        )
