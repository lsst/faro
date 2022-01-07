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
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum

from lsst.faro.utils.tex_table import calculateTEx

import astropy.units as u
import numpy as np

__all__ = ("TExTableConfig", "TExTableTask", "NumSourcesConfig", "NumSourcesTask")

class NumSourcesConfig(Config):
    doPrimary = Field(
        doc="Only count sources where detect_isPrimary is True.",
        dtype=bool,
        default=False,
    )

    selectorActions = ConfigurableActionStructField(
        doc="Which selectors to use to narrow down the data (independent of band).",
        default={},
        #default={"sourceSelector": selectors.StarIdentifier},
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
        catalog = selectors.applySelectors(catalog,self.config.selectorActions,currentBands=kwargs["currentBands"])
        if self.config.doPrimary:
            nSources = np.sum(catalog["detect_isPrimary"] is True)
        else:
            nSources = len(catalog)
        self.log.info("Number of sources (nSources) = %i" % nSources)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)

class TExTableConfig(Config):
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
    raColumn = Field(doc="RA column", dtype=str, default="coord_ra")
    decColumn = Field(doc="Dec column", dtype=str, default="coord_dec")
    ixxColumn = Field(doc="Ixx column", dtype=str, default="ixx")
    ixyColumn = Field(doc="Ixy column", dtype=str, default="ixy")
    iyyColumn = Field(doc="Iyy column", dtype=str, default="iyy")
    ixxPsfColumn = Field(doc="Ixx PSF column", dtype=str, default="ixxPSF")
    ixyPsfColumn = Field(doc="Ixy PSF column", dtype=str, default="ixyPSF")
    iyyPsfColumn = Field(doc="Iyy PSF column", dtype=str, default="iyyPSF")
    extendednessColumn = Field(doc="Extendedness column", dtype=str, default="extendedness")
    psfFluxColumn = Field(doc="PsfFlux column", dtype=str, default="psfFlux")
    psfFluxErrColumn = Field(doc="PsfFluxErr column", dtype=str, default="psfFluxErr")
    deblend_nChildColumn = Field(doc="nChild column", dtype=str, default="deblend_nChild")
    # Eventually want to add option to use only PSF reserve stars


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
        self, metricName, catalog, band=None
    ):

        self.log.info("Measuring %s", metricName)

        # If accessing objectTable_tract, we need to append the band name at
        #   the beginning of the column name.
        if band is not None:
            prependString = band
        else:
            prependString = None
        
        # filter catalog 
        catalog = selectors.applySelectors(catalog,[self.config.selectorActions,self.config.perBandSelectorActions])
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
