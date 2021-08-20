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
from lsst.faro.utils.tex import calculateTEx
from lsst.faro.utils.calibrated_catalog import CalibratedCatalog

import astropy.units as u
import numpy as np
from typing import Dict, List

__all__ = ("WPerpConfig", "WPerpTask", "TExConfig", "TExTask")


class WPerpConfig(Config):
    # These are cuts to apply to the r-band only:
    bright_rmag_cut = Field(
        doc="Bright limit of catalog entries to include", dtype=float, default=17.0
    )
    faint_rmag_cut = Field(
        doc="Faint limit of catalog entries to include", dtype=float, default=23.0
    )


class WPerpTask(Task):
    ConfigClass = WPerpConfig
    _DefaultName = "WPerpTask"

    def run(
        self, metricName: str, data: Dict[str, List[CalibratedCatalog]],
    ):
        self.log.info("Measuring %s", metricName)
        bands = set("gri")

        if bands.issubset(set(data.keys())):
            data = {b: data[b] for b in bands}
            rgicatAll = makeMatchedPhotom(data)
            magcut = (rgicatAll["base_PsfFlux_mag_r"] < self.config.faint_rmag_cut) & (
                rgicatAll["base_PsfFlux_mag_r"] > self.config.bright_rmag_cut
            )
            self.log.info("Merged multiband catalog is %d rows (%d match mag cut)",
                          len(rgicatAll), np.sum(magcut))
            rgicat = rgicatAll[magcut]
            extVals = extinction_corr(rgicat, bands)

            wPerp = self.calcWPerp(metricName, rgicat, extVals)
            return wPerp
        else:
            return Struct(measurement=Measurement(metricName, np.nan * u.mmag))

    def calcWPerp(self, metricName: str, phot: SourceCatalog, extinctionVals):
        p1, p2, p1coeffs, p2coeffs = stellarLocusResid(
            *[phot[f"base_PsfFlux_mag_{b}"] - extinctionVals[f"A_{b}"] for b in 'gri'],
        )

        if np.size(p2) > 2:
            p2_rms = calcQuartileClippedStats(p2).rms * u.mag
            extras = {
                "p1_coeffs": Datum(
                    p1coeffs * u.Unit(""),
                    label="p1_coefficients",
                    description="p1 coeffs from wPerp fit",
                ),
                "p2_coeffs": Datum(
                    p2coeffs * u.Unit(""),
                    label="p2_coefficients",
                    description="p2_coeffs from wPerp fit",
                ),
            }

            return Struct(
                measurement=Measurement(metricName, p2_rms.to(u.mmag), extras=extras)
            )
        else:
            return Struct(measurement=Measurement(metricName, np.nan * u.mmag))


class TExConfig(Config):
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
    columnPsf = Field(
        doc="Column to use for PSF model shape moments",
        dtype=str,
        default="base_SdssShape_psf",
    )
    column = Field(
        doc="Column to use for shape moments", dtype=str, default="base_SdssShape"
    )
    # Eventually want to add option to use only PSF reserve stars


class TExTask(Task):
    ConfigClass = TExConfig
    _DefaultName = "TExTask"

    def run(
        self, metricName, data: Dict[str, List[CalibratedCatalog]]
    ):
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
