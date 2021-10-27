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
from lsst.pex.config import Config, Field
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum

from lsst.faro.utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats
from lsst.faro.utils.matcher import makeMatchedPhotom
from lsst.faro.utils.extinction_corr import extinction_corr
from lsst.faro.utils.tex_table import calculateTEx
from lsst.faro.utils.calibrated_catalog import CalibratedCatalog

import astropy.units as u
import numpy as np
from typing import Dict, List

__all__ = ("TExTableConfig", "TExTableTask")


class TExTableConfig(Config):
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
        default=False,
    )
    raColumn = Field(doc="RA column", dtype=str, default="coord_ra")
    decColumn = Field(doc="Dec column", dtype=str, default="coord_dec")
    ixxColumn = Field(doc="Ixx column", dtype=str, default="Ixx")
    ixyColumn = Field(doc="Ixy column", dtype=str, default="Ixy")
    iyyColumn = Field(doc="Iyy column", dtype=str, default="Iyy")
    ixxPsfColumn = Field(doc="Ixx PSF column", dtype=str, default="IxxPsf")
    ixyPsfColumn = Field(doc="Ixy PSF column", dtype=str, default="IxyPsf")
    iyyPsfColumn = Field(doc="Iyy PSF column", dtype=str, default="IyyPsf")
    extendednessColumn = Field(doc="Extendedness column", dtype=str, default="extendedness")
    psfFluxColumn = Field(doc="PsfFlux column", dtype=str, default="PsfFlux")
    psfFluxErrColumn = Field(doc="PsfFluxErr column", dtype=str, default="PsfFluxErr")
    deblend_nChildColumn = Field(doc="nChild column", dtype=str, default="Deblend_nChild")
    # Eventually want to add option to use only PSF reserve stars


class TExTableTask(Task):
    ConfigClass = TExTableConfig
    _DefaultName = "TExTableTask"

    def run(
        self, metricName, catalog
    ):
        import pdb; pdb.set_trace()

        bands = data.keys()
        if len(bands) != 1:
            raise RuntimeError(f'TEx task got bands: {bands} but expecting exactly one')
        else:
            data = data[list(bands)[0]]

        self.log.info("Measuring %s", metricName)

        result = calculateTEx(data, self.config)
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
