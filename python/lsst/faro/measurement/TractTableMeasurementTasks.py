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

from lsst.pex.config import Field, DictField
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum

from lsst.faro.base.ConfigBase import MeasurementTaskConfig
import lsst.faro.utils.selectors as selectors
from lsst.faro.utils.tex_table import calculateTEx
from lsst.faro.utils.extinction_corr import extinctionCorrTable
from lsst.faro.utils.stellarLocus import stellarLocusFit
from lsst.faro.utils.stats_utils import calcQuartileClippedStats

# from scipy.stats import median_abs_deviation
import astropy.units as u
from astropy.coordinates import SkyCoord
import numpy as np

__all__ = ("TExTableConfig", "TExTableTask",
           "wPerpTableConfig", "wPerpTableTask")


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
                label="Correlation Uncertianty",
                description="Correlation Uncertainty.",
            )
        else:
            extras = None
        return Struct(
            measurement=Measurement(
                metricName, np.mean(np.abs(result["corr"])), extras=extras
            )
        )


class wPerpTableConfig(MeasurementTaskConfig):
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

    #bright_rmag_cut = Field(
    #    doc="Bright magnitude limit to select", dtype=float, default=17.0
    #)
    #faint_rmag_cut = Field(
    #    doc="Faint magnitude limit to select", dtype=float, default=23.0
    #)
    columns = DictField(
        doc="""Columns required for metric calculation. Should be all columns in SourceTable contexts,
        and columns that do not change name with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={"ra": "coord_ra",
                 "dec": "coord_dec",
                }
    )
    columnsBand = DictField(
        doc="""Columns required for metric calculation that change with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={"psfFlux": "psfFlux"
                } ##### Check what columns are needed!
    )
    stellarLocusFitDict = DictField(
        doc="The parameters to use for the stellar locus fit. The default parameters are examples and are "
            "not useful for any of the fits. The dict needs to contain xMin/xMax/yMin/yMax which are the "
            "limits of the initial box for fitting the stellar locus, mHW and bHW are the initial "
            "intercept and gradient for the fitting.",
        keytype=str,
        itemtype=float,
        default={"xMin": 0.28, "xMax": 1.0, "yMin": 0.02, "yMax": 0.48, "mHW": 0.52, "bHW": -0.08}
    )


class wPerpTableTask(Task):
    """Class to perform the wPerp calculation on a parquet table data
    object.

    Parameters
    ----------
    None

    Output:
    ----------
    wPerp stellar locus metric with defined configuration.
    """

    ConfigClass = wPerpTableConfig
    _DefaultName = "wPerpTableTask"

    def run(
        self, metricName, catalog, currentBands, **kwargs
    ):

        self.log.info("Measuring %s", metricName)

        # Validation to require that gri bands are in the input set
        if set(currentBands).issuperset(set(['g', 'r', 'i'])) is False:
            self.log.warn("Data in gri bands required for wPerp calculation.")
            return Struct(measurement=Measurement(metricName, np.nan * u.mmag))

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
       
        # Filter based on configured r-mag limits:
        # rmag_tmp = (catalog.r_psfFlux.values*u.nJy).to(u.ABmag).value
        # magfilter = (rmag_tmp < self.config.faint_rmag_cut) &\
        #             (rmag_tmp > self.config.bright_rmag_cut)
        # catalog = catalog[magfilter]

        # Create a SkyCoord object to get the extinction:
        sc = SkyCoord(catalog['coord_ra']*u.deg, catalog['coord_dec']*u.deg)

        extVals = extinctionCorrTable(sc, currentBands)

        g0 = (catalog.g_psfFlux.values*u.nJy).to(u.ABmag).value - extVals['A_g']
        r0 = (catalog.r_psfFlux.values*u.nJy).to(u.ABmag).value - extVals['A_r']
        i0 = (catalog.i_psfFlux.values*u.nJy).to(u.ABmag).value - extVals['A_i']

        # Call the stellar locus fit function:
        p1vals, p2vals, fitParams = stellarLocusFit(g0-r0, r0-i0,
                                                    self.config.stellarLocusFitDict)

        # Ivezic+2004 defines wPerp between -0.2 < p1 < 0.6.
        # Also, do some expanded color cuts to get rid of large color outliers that
        #   skew the rms.
        magcushion = 0.2
        okp1 = (p1vals > -0.2) & (p1vals < 0.6) &\
               ((r0-i0) < fitParams['yMax']+magcushion) &\
               ((r0-i0) > fitParams['yMin']-magcushion) &\
               ((g0-r0) < fitParams['xMax']+magcushion) &\
               ((g0-r0) > fitParams['xMin']-magcushion)

        if np.size(p2vals[okp1]) > 2:
            p2_mad = calcQuartileClippedStats(p2vals[okp1]).mad * u.mag
            extras = {
                "wPerp_coeffs": Datum(
                    [fitParams['mODR2'], fitParams['bODR2']],
                    unit=u.Unit(""),
                    label="wPerp_coefficients",
                    description="Slope, intercept coeffs from wPerp fit",
                ),
            }
            return Struct(
                measurement=Measurement(metricName, p2_mad.to(u.mmag), extras=extras)
            )
        else:
            return Struct(measurement=Measurement(metricName, np.nan * u.mmag))
