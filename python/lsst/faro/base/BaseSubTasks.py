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

from lsst.afw.table import SourceCatalog
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config, Field
from lsst.verify import Measurement

from lsst.faro.utils.matcher import mergeCatalogs
from lsst.faro.utils.calibrated_catalog import CalibratedCatalog

import astropy.units as u
import numpy as np
from typing import Dict, List

__all__ = (
    "NumSourcesTask",
    "NumSourcesMatchedTask",
    "NumSourcesMergeTask",
    "NumpySummaryConfig",
    "NumpySummaryTask",
)


class NumSourcesConfig(Config):
    doPrimary = Field(
        doc="Only count sources where detect_isPrimary is True.",
        dtype=bool,
        default=False,
    )


class NumSourcesTask(Task):
    r"""Simple default task to count the number of sources/objects in catalog."""

    ConfigClass = NumSourcesConfig
    _DefaultName = "numSourcesTask"

    def run(self, metricName, catalog, **kwargs):
        """Run NumSourcesTask

        Parameters
        ----------
        metricName : `str`
            The name of the metric to measure.
        catalog : `dict`
            `lsst.afw.table` Catalog type
        kwargs
            Extra keyword arguments used to construct the task.

        Returns
        -------
        measurement : `Struct`
            The measured value of the metric.
        """
        self.log.info("Measuring %s", metricName)
        if self.config.doPrimary:
            nSources = np.sum(catalog["detect_isPrimary"] is True)
        else:
            nSources = len(catalog)
        self.log.info("Number of sources (nSources) = %i" % nSources)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)


class NumSourcesMatchedTask(NumSourcesTask):
    r"""Extension of NumSourcesTask to count sources in a matched catalog"""

    # The only purpose of this task is to call NumSourcesTask's run method
    # TODO: Review the necessity of this in DM-31061
    def run(self, metricName: str, matchedCatalog: SourceCatalog, **kwargs):
        return super().run(metricName, matchedCatalog, **kwargs)


class NumSourcesMergeTask(Task):

    ConfigClass = Config
    _DefaultName = "numSourcesMergeTask"

    def run(self, metricName: str, data: Dict[str, List[CalibratedCatalog]]):
        bands = list(data.keys())
        if len(bands) != 1:
            raise RuntimeError(f'NumSourcesMergeTask task got bands: {bands} but expecting exactly one')
        else:
            data = data[list(bands)[0]]
        self.log.info("Measuring %s", metricName)
        catalog = mergeCatalogs(
            [x.catalog for x in data],
            [x.photoCalib for x in data],
            [x.astromCalib for x in data],
        )
        nSources = len(catalog)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)


class NumpySummaryConfig(Config):
    summary = Field(
        dtype=str, default="median", doc="Aggregation to use for summary metrics"
    )


class NumpySummaryTask(Task):

    ConfigClass = NumpySummaryConfig
    _DefaultName = "numpySummaryTask"

    def run(self, measurements, agg_name, package, metric):
        agg = agg_name.lower()
        if agg == "summary":
            agg = self.config.summary
        self.log.info("Computing the %s of %s_%s values", agg, package, metric)

        if len(measurements) == 0:
            self.log.info("Received zero length measurements list.  Returning NaN.")
            # In the case of an empty list, there is nothing we can do other than
            # to return a NaN
            value = u.Quantity(np.nan)
        else:
            unit = measurements[0].quantity.unit
            value = getattr(np, agg)(
                u.Quantity(
                    [x.quantity for x in measurements if np.isfinite(x.quantity)]
                )
            )
            # Make sure return has same unit as inputs
            # In some cases numpy can return a NaN and the unit gets dropped
            value = value.value * unit
        return Struct(
            measurement=Measurement(
                f"metricvalue_{agg_name.lower()}_{package}_{metric}", value
            )
        )
