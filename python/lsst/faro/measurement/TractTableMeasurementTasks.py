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
import numpy as np
from typing import Dict, List
from lsst.pex.config import Config, Field, ChoiceField
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum
from lsst.faro.utils.abs_astrom_table import matchCatsKDTree
from lsst.faro.utils.morphology import calculateStarGal


__all__ = ("starGalTableConfig", "starGalTableTask")

class starGalTableConfig(Config):
    brightSnrMin = Field(
        doc="Minimum median SNR for a source to be considered bright.",
        dtype=float,
        default=200,
    )
    brightSnrMax = Field(
        doc="Maximum median SNR for a source to be considered bright.",
        dtype=float,
        default=np.Inf,
    )
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
        allowed={"starDepth":"compute metric on ra * cos(dec)",
                "dec": "compute metric on dec",
                "total": "compue metric on angular seperation"},
        default="starDepth",
    ) 
    writeExtras = Field(
        doc="Write out the astrometric residuals and rms values for debugging.",
        dtype=bool,
        default=False,
    )    
    raColumn = Field(doc="RA column", dtype=str, default="coord_ra")
    decColumn = Field(doc="Dec column", dtype=str, default="coord_dec")
    refRaColumn = Field(doc="refcat RA column", dtype=str, default="ra")
    refDecColumn = Field(doc="refcat DEC column", dtype=str, default="dec")
    extendednessColumn = Field(doc="Extendedness column", dtype=str, default="extendedness")
    psfFluxColumn = Field(doc="PsfFlux column", dtype=str, default="psfFlux")
    psfFluxErrColumn = Field(doc="PsfFluxErr column", dtype=str, default="psfFluxErr")
    deblend_nChildColumn = Field(doc="nChild column", dtype=str, default="deblend_nChild")
    # Eventually want to add option to use only PSF reserve stars/calib_astrometry_used


class starGalTableTask(Task):
    """A Task that computes the AA1 absolute astrometric deviation metric from an
    input set measured sources and a reference catalog. 

    Notes
    -----
    The intended usage is to retarget the run method of
    `lsst.faro.measurement.TractTableMeasurementTask` to AA1Task.
    This metric is calculated on a set of visits.
    """
    ConfigClass = starGalTableConfig
    _DefaultName = "starGalTableTask"
    def __init__(self, config: starGalTableConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)

    def run(
        self, metricName, catalog, refCatCorrected, **kwargs #refcat is hooked in as part of TractTableMeasurement
    ):
        #signal to noise cut on catalog
        #will want to replace with lsst.faro.utils.filtermatchestable.py when that becomes available
        
        #bright
        snr = catalog[self.config.psfFluxColumn]/catalog[self.config.psfFluxErrColumn]
        sel = ( snr > self.config.brightSnrMin) & (snr < self.config.brightSnrMax) # should these be <= or <

        #star
        sel &= (catalog[self.config.extendednessColumn] < 0.9)
        
        #deblend?
        # do we want to filter on calib_astrometry_used? 

        catalog = catalog[sel]

        catalog,refCatCorrected, sep = matchCatsKDTree(catalog,refCatCorrected,self.config)

        lenCat = len(catalog[self.config.psfFluxColumn])

        if lenCat < self.config.nMinAbsAstromMatches:
            self.log.info("too few matches: %s", lenCat)
            return Struct(measurement=Measurement(metricName, np.nan * u.mas)) 
        
        self.log.info("Matched %s objects", lenCat)
        
        self.log.info("Measuring {}".format( metricName))
        
        AA1 = {"bot":lenCat}#calculateAA1(catalog, refCatCorrected, self.config)

        if "medianAbsDeviation" in AA1.keys():
            self.log.info("Median Absolute Deviation: %s",AA1['medianAbsDeviation'])
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