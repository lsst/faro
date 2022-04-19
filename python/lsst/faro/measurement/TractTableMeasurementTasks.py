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
from lsst.faro.utils.astrometry import calculateAA1

import astropy.units as u
import numpy as np
from smatch.matcher import Matcher

__all__ = ("AA1TableConfig",
           "AA1TableTask",
           "FluxStatisticConfig",
           "FluxStatisticTask",
           "TExTableConfig",
           "TExTableTask", )


class AA1TableConfig(MeasurementTaskConfig):
    """Class to configure the AA1 metric measurement task.
    """
    maxSep = Field(
        doc="Maximum seperation for a source to be considered a match in degrees",
        dtype=float,
        default=0.5/3600,
    )
    nMinAbsAstromMatches = Field(
        doc="Minimum number of objects required for median error in absolute position",
        dtype=int,
        default=10,
    )
    metricColumn = ChoiceField(
        doc="column to compute median of abs(seperation). Choices are ra, dec, or total",
        dtype=str,
        allowed={"ra": "compute metric on ra * cos(dec)",
                 "dec": "compute metric on dec",
                 "total": "compue metric on angular seperation"},
        default="ra",
    )
    refColumns = DictField(
        doc="Columns in reference catalog required for metric calculation.",
        keytype=str,
        itemtype=str,
        default={"ra": "ra",
                 "dec": "dec",
                 }
    )
    columns = DictField(
        doc="""Columns required for metric calculation. Should be all columns in SourceTable contexts,
        and columns that do not change name with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={"ra": "coord_ra",
                 "dec": "coord_dec"
                 }
    )
    writeExtras = Field(
        doc="Write out the astrometric residuals and rms values for debugging.",
        dtype=bool,
        default=False,
    )
    # Eventually want to add option to use only PSF reserve stars/calib_astrometry_used


class AA1TableTask(Task):
    """A Task that computes the AA1 absolute astrometric deviation metric from
    an input set measured sources and a reference catalog.

    Output
    ------
    AA1 metric with defined configuration.
    """
    ConfigClass = AA1TableConfig
    _DefaultName = "AA1TableTask"

    def __init__(self, config: AA1TableConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)

    def run(
        self, metricName, catalog, refCat, **kwargs
    ):
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=kwargs["currentBands"])

        with Matcher(refCat["ra"], refCat["dec"]) as matcher:
            idx = matcher.query_knn(catalog["coord_ra"], catalog["coord_dec"],
                                    distance_upper_bound=self.config.maxSep)

        catalog = catalog.iloc[idx < len(refCat), :]

        refCat = refCat.iloc[idx[idx < len(refCat)], :]

        lenCat = len(catalog[self.config._getColumnName("ra")])

        if lenCat < self.config.nMinAbsAstromMatches:
            self.log.info("too few matches: %s", lenCat)
            return Struct(measurement=Measurement(metricName, np.nan * u.mas))

        self.log.info("Matched %s objects", lenCat)

        self.log.info("Measuring {}".format(metricName))

        AA1 = calculateAA1(catalog, refCat, self.config)

        if "medianAbsDeviation" in AA1.keys():
            self.log.info("Median Absolute Deviation: %s", AA1['medianAbsDeviation'])
            if self.config.writeExtras:
                extras = {
                    "rms": Datum(
                        AA1["rms"],
                        label="RMS",
                        description="rms of sources used to calculate metric",
                    ),
                    "count": Datum(
                        AA1["count"] * u.count,
                        label="count",
                        description="Number of detections used to calculate metric",
                    ),
                    "median": Datum(
                        AA1["median"],
                        label="median",
                        description="median astrometric residual",
                    ),
                }
                return Struct(
                    measurement=Measurement(metricName, AA1["medianAbsDeviation"], extras=extras)
                )
            else:
                return Struct(measurement=Measurement(metricName, AA1["medianAbsDeviation"]))
        else:
            return Struct(measurement=Measurement(metricName, np.nan * u.mas))


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
                 "iyyPsf": "iyy"
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
