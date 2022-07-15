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

import lsst.afw.math as afwMath
from lsst.pex.config import Field, DictField, ChoiceField
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum

from lsst.faro.base.ConfigBase import MeasurementTaskConfig
import lsst.faro.utils.selectors as selectors
from lsst.faro.utils.tex_table import calculateTEx

import astropy.units as u
import numpy as np

__all__ = (
    "TExTableConfig", "TExTableTask",
    "FluxStatisticConfig", "FluxStatisticTask",
    "ColumnMeasurementConfig", "ColumnMeasurementTask"
)


class TExTableConfig(MeasurementTaskConfig):
    """Class to organize the yaml configuration parameters to be passed to
    TExTableTask when using a parquet table input. All values needed to perform
    TExTableTask have default values set below.

    Optional Input (yaml file)
    ----------
    Column names specified as str in yaml configuration for TeX task. These are
    the desired column names to be passed to the calcuation. If you wish to use
    values other than the default values specified below, add the following e.g.
    line to the yaml file:

    config.measure.raColumn = "coord_ra_new"
    """

    minSep = Field(
        doc="Inner radius of the annulus in arcmin", dtype=float, default=0.25
    )
    maxSep = Field(
        doc="Outer radius of the annulus in arcmin", dtype=float, default=1.0
    )
    nbins = Field(doc="Number of log-spaced angular bins", dtype=int, default=10)
    rhoStat = Field(doc="Rho statistic to be computed", dtype=int, default=1)
    shearConvention = Field(
        doc="Use shear ellipticity convention rather than distortion",
        dtype=bool,
        default=True,
    )
    columns = DictField(
        doc="""Columns required for metric calculation. Should be all columns in SourceTable contexts,
        and columns that do not change name with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={"ra": "coord_ra",
                 "dec": "coord_dec",
                 "ixx": "ixx",
                 "ixy": "ixx",
                 "iyy": "ixx",
                 "ixxPsf": "ixx",
                 "ixyPsf": "ixx",
                 "iyyPsf": "iyy",
                 "deblend_nChild": "deblend_nChild"
                 }
    )
    columnsBand = DictField(
        doc="""Columns required for metric calculation that change with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={}
    )


class TExTableTask(Task):
    """Class to perform the tex_table calculation on a parquet table data
    object.

    Parameters
    ----------
    None

    Output:
    ----------
    TEx metric with defined configuration.
    """

    ConfigClass = TExTableConfig
    _DefaultName = "TExTableTask"

    def run(
        self, metricName, catalog, currentBands, **kwargs
    ):

        self.log.info("Measuring %s", metricName)

        # If accessing objectTable_tract, we need to append the band name at
        #   the beginning of the column name.
        if currentBands is not None:
            prependString = currentBands
        else:
            prependString = None

        # filter catalog
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=currentBands)

        result = calculateTEx(catalog, self.config, prependString)
        if "corr" not in result.keys():
            return Struct(measurement=Measurement(metricName, np.nan * u.Unit("")))

        writeExtras = True
        if writeExtras:
            extras = {}
            extras["radius"] = Datum(
                result["radius"], label="radius", description="Separation (arcmin)."
            )
            extras["corr"] = Datum(
                result["corr"], label="Correlation", description="Correlation."
            )
            extras["corrErr"] = Datum(
                result["corrErr"],
                label="Correlation Uncertainty",
                description="Correlation Uncertainty.",
            )
        else:
            extras = None
        return Struct(
            measurement=Measurement(
                metricName, np.mean(np.abs(result["corr"])), extras=extras
            )
        )


class FluxStatisticConfig(MeasurementTaskConfig):
    """Class to configure the flux statistic measurement task.
    """

    statistic = Field(
        doc="Statistic name to use to generate the metric (from `~lsst.afw.math.Property`).",
        dtype=str,
        default="MEAN",
    )
    numSigmaClip = Field(
        doc="Rejection threshold (sigma) for statistics clipping.",
        dtype=float,
        default=5.0,
    )
    clipMaxIter = Field(
        doc="Maximum number of clipping iterations to apply.",
        dtype=int,
        default=3,
    )


class FluxStatisticTask(Task):
    """Class to perform flux statistic calculations on parquet table data.

    Output
    ------
    Flux metric with defined configuration.
    """

    ConfigClass = FluxStatisticConfig
    _DefaultName = "FluxStatisticTask"

    def run(
        self, metricName, catalog, currentBands, **kwargs
    ):

        self.log.info("Measuring %s", metricName)

        # filter catalog using selectors
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=currentBands)

        # extract flux value column
        all_columns = [x for x in self.config.columns.values()]
        for col in self.config.columnsBand.values():
            all_columns.append(f"{currentBands}_{col}")
        fluxes = catalog[all_columns].iloc[:, 0].to_numpy()

        # calculate statistic
        statisticToRun = afwMath.stringToStatisticsProperty(self.config.statistic)
        statControl = afwMath.StatisticsControl(self.config.numSigmaClip,
                                                self.config.clipMaxIter,)
        result = afwMath.makeStatistics(fluxes, statisticToRun, statControl).getValue()

        # return result
        return Struct(
            measurement=Measurement(
                metricName, result*u.nanojansky, extras=None
            )
        )


class ColumnMeasurementConfig(MeasurementTaskConfig):
    # choices from lsst.afw.math.Statistic.h
    statistic = ChoiceField(
        doc="Name of statistic to calculate",
        dtype=str,
        default="SUM",
        allowed={
            "NOTHING": "We don't want anything",
            "ERRORS": "Include errors of requested quantities",
            "NPOINT": "number of sample points",
            "MEAN": "estimate sample mean",
            "STDEV": "estimate sample standard deviation",
            "VARIANCE": "estimate sample variance",
            "MEDIAN": "estimate sample median",
            "IQRANGE": "estimate sample inter-quartile range",
            "MEANCLIP": "estimate sample N-sigma clipped mean (N set in StatisticsControl, default=3)",
            "STDEVCLIP": "estimate sample N-sigma clipped stdev (N set in StatisticsControl, default=3)",
            "VARIANCECLIP": "estimate sample N-sigma clipped variance",
            "MIN": "estimate sample minimum",
            "MAX": "estimate sample maximum",
            "SUM": "find sum of pixels in the image",
            "MEANSQUARE": "find mean value of square of pixel values",
            "ORMASK": "get the or-mask of all pixels used.",
            "NCLIPPED": "number of clipped points",
            "NMASKED": "number of masked points",
        }
    )

    numSigmaClip = Field(
        doc="Rejection threshold (sigma) for statistics clipping.",
        dtype=float,
        default=3,
    )

    clipMaxIter = Field(
        doc="Maximum number of clipping iterations to apply.",
        dtype=int,
        default=3,
    )

    units = Field(
        doc="Name of the (astropy) units to use",
        dtype=str,
        default="count",
    )


class ColumnMeasurementTask(Task):
    ConfigClass = ColumnMeasurementConfig
    _DefaultName = "ColumnMeasurementTask"

    def run(
        self, metricName, catalog, **kwargs
    ):
        self.log.info("Measuring %s", metricName)

        # filter catalog using selectors
        catalog = selectors.applySelectors(
            catalog,
            self.config.selectorActions,
        )

        nColumns = len(self.config.columns)
        if nColumns == 0:
            # Return the number of rows matching the selection criteria
            result = len(catalog)
        elif nColumns == 1:
            # Calculate the statistic on the column
            data = catalog[list(self.config.columns.values())[0]].to_numpy()
            statisticToRun = afwMath.stringToStatisticsProperty(self.config.statistic)
            statControl = afwMath.StatisticsControl(self.config.numSigmaClip, self.config.clipMaxIter)
            result = afwMath.makeStatistics(data, statisticToRun, statControl).getValue()
        else:
            raise ValueError("Expected either 0 or 1 columns to measure {metricName}, received {nColumns}")

        # set the units for the measurement
        units = getattr(u, self.config.units)

        # return result
        return Struct(
            measurement=Measurement(
                metricName, result*units, extras=None
            )
        )
