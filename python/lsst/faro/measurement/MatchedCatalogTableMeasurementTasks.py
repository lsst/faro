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

import astropy.units as u
import numpy as np

__all__ = (
    "PA1TableConfig",
    "PA1TableTask",
)
class PA1TableConfig(MeasurementTaskConfig):
    """Config fields for the PA1 photometric repeatability metric.
    """
    nMinPhotRepeat = Field(
        doc="Minimum number of objects required for photometric repeatability.",
        dtype=int,
        default=50,
    )
    meanSNRCut = Field(
        doc="Minimum mean Signal to Noise ratio for source to be incuded in PA1",
        dtype=int,
        default=50,
    )
    writeExtras = Field(
        doc="Write out the magnitude residuals and rms values for debugging.",
        dtype=bool,
        default=False,
    )
    columns = DictField(
        doc="""Columns required for metric calculation. Should be all columns in SourceTable contexts,
        and columns that do not change name with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={"ra": "coord_ra",
                 "dec": "coord_dec",
                 "flux": "psfFlux",
                 "fluxErr":"psfFluxErr",
                 "objectID":"obj_index",
                 }
    )
    columnsBand = DictField(
        doc="""Columns required for metric calculation that change with band in ObjectTable contexts""",
        keytype=str,
        itemtype=str,
        default={}
    ) 

class PA1TableTask(Task):
    """Class to perform the PA1 calculation on a ForcedSource parquet Object Table. 
    PA1 is the photometric repeatability metric from an input set of multiple 
    visits of the same field.

    Parameters
    ----------
    None

    Output:
    ----------
    PA1 metric with defined configuration.
    """

    ConfigClass = PA1TableConfig
    _DefaultName = "PA1TableTask"

    def run(
        self, metricName, catalog, currentBands, **kwargs
    ):

        self.log.info("Measuring %s", metricName)
        
        # filter catalog
        # Select bright isolated s/n filter 20 for pre aggregate cuts
        
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=currentBands)
        
        objectidColumn=self.config._getColumnName("objectID", currentBands)
        fluxColumn=self.config._getColumnName("flux", currentBands)
        fluxErrColumn=self.config._getColumnName("fluxErr", currentBands)
        
        magColumn=self.config._getColumnName("flux", currentBands).replace("Flux","Mag")
        mags=(catalog.loc[:,fluxColumn].values*u.nJy).to(u.ABmag).value
        catalog=catalog.assign(**{magColumn:mags})
        
        # compute aggregated values
        photRepeatFrame=catalog.groupby(
                                        objectidColumn
                                    ).aggregate(
                                            {magColumn:[
                                                np.nanstd,
                                                np.count_nonzero,
                                                np.mean
                                            ]
                                        }
                                    ).rename(
                                        columns={
                                            'nanstd': 'rms',
                                            'count_nonzero':'count',
                                            "mean":"magMean"
                                        }
                                    )
        photRepeatFrame.loc[:,"meanSnr"]=catalog.groupby(
                                                   objectidColumn
                                                ).apply(
                                                    lambda row: np.mean(row[fluxColumn]/row[fluxErrColumn])
                                                    )
                                  
        photRepeatFrame.loc[:,"residuals"]=catalog.groupby(
                                                    objectidColumn
                                                )[magColumn].apply(lambda x:np.array(x-np.mean(x)))
        okRms=(photRepeatFrame[(magColumn,"count")] > 2)
        okMeanSNR=(photRepeatFrame['meanSnr']> self.config.meanSNRCut)

        #compute pa1 metric          
        repeatability=(np.median(photRepeatFrame.loc[okRms & okMeanSNR,(magColumn,"rms")])*u.mag).to(u.mmag)
        self.log.info("Median rms = %i mmag" % repeatability.value)
        if ((magColumn, "magMean") in photRepeatFrame.columns) and (okRms & okMeanSNR).sum() > self.config.nMinPhotRepeat:
            if self.config.writeExtras:
                extras = {
                    "rms": Datum(
                        (photRepeatFrame[(magColumn,"rms")].values*u.mag).to(u.mmag),
                        label="RMS",
                        description="Photometric repeatability rms for each star.",
                    ),
                    "count": Datum(
                        photRepeatFrame[(magColumn,"count")].values * u.count,
                        label="count",
                        description="Number of detections used to calculate repeatability.",
                    ),
                    "mean_mag": Datum(
                        photRepeatFrame[(magColumn,"magMean")].values * u.mag,
                        label="mean_mag",
                        description="Mean magnitude of each star.",
                    ),
                }
                return Struct(
                    measurement=Measurement("PA1", repeatability, extras=extras)
                )
            else:
                return Struct(measurement=Measurement("PA1", repeatability))
        else:
            return Struct(measurement=Measurement("PA1", np.nan * u.mmag))
