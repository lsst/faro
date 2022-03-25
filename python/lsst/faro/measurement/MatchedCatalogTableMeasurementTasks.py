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
from lsst.faro.utils.phot_repeat_table import calcPhotRepeatTable

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
        doc="""Minimum mean Signal to Noise ratio over all observations of a source 
            for source to be incuded in PA1""",
        dtype=float,
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
                 "fluxFlag":"psfFluxFlag",
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
    """Class to perform the PA1 calculation on a Matched parquet Source Table. 
    PA1 is the photometric repeatability metric from an input set of multiple 
    visits of the same field.

    Parameters
    ----------
    Matched Catalog: pandas dataframe with group index column

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
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=currentBands)
        
        objectidColumn=self.config._getColumnName("objectID", currentBands)
        fluxColumn=self.config._getColumnName("flux", currentBands)
        fluxErrColumn=self.config._getColumnName("fluxErr", currentBands)
        
        # compute aggregated values
        photRepeatFrame=calcPhotRepeatTable(catalog,
                                            objectidColumn,
                                            fluxColumn,
                                            fluxErrColumn,
                                            residuals=False
                                            )

        okRms=(photRepeatFrame[("count")] > 2)
        okMeanSNR=(photRepeatFrame['meanSnr']> self.config.meanSNRCut)
        
        #compute PA1 metric          
        repeatability=(np.median(photRepeatFrame.loc[okRms & okMeanSNR,("rms")])*u.mag).to(u.mmag)
        self.log.info("Median rms = %i mmag" % repeatability.value)

        if ("magMean" in photRepeatFrame.columns) and ((okRms & okMeanSNR).sum() > self.config.nMinPhotRepeat):
            if self.config.writeExtras:
                extras = {
                    "rms": Datum(
                        (photRepeatFrame["rms"].values*u.mag).to(u.mmag),
                        label="RMS",
                        description="Photometric repeatability rms for each star.",
                    ),
                    "count": Datum(
                        photRepeatFrame["count"].values * u.count,
                        label="count",
                        description="Number of detections used to calculate repeatability.",
                    ),
                    "mean_mag": Datum(
                        photRepeatFrame["magMean"].values * u.mag,
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

class PF1TableConfig(MeasurementTaskConfig):
    nMinPhotRepeat = Field(
        doc="Minimum number of objects required for photometric repeatability.",
        dtype=int,
        default=50,
    )
    meanSNRCut = Field(
        doc="""Minimum mean Signal to Noise ratio over all observations of a source 
            for source to be incuded in PA1""",
        dtype=float,
        default=50,
    )
    writeExtras = Field(
        doc="Write out the magnitude residuals and rms values for debugging.",
        dtype=bool,
        default=False,
    )
    # The defaults for threshPA2 correspond to the SRD "design" thresholds.
    threshPA2 = Field(
        doc="Threshold in mmag for PF1 calculation.", dtype=float, default=15.0
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


class PF1TableTask(Task):
    """A Task that computes PF1, the percentage of photometric repeatability measurements
    that deviate by more than PA2 mmag from the mean.

    Notes
    -----
    The intended usage is to retarget the run method of
    `lsst.faro.measurement.TractMatchedCatalogTableMeasurementTask` to PF1Task. This Task uses the
    same set of photometric residuals that are calculated for the PA1 metric.
    This metric is calculated on a set of matched visits, and aggregated at the tract level.
    """

    ConfigClass = PF1TableConfig
    _DefaultName = "PF1TableTask"

    def run(
        self, metricName, catalog, currentBands, **kwargs
    ):
        """Calculate the percentage of outliers in the photometric repeatability values.

        Parameters
        ----------
        Matched Catalog: 
            pandas dataframe of SourceTable observations with group index column

        metricName : `str`
            The name of the metric.

        Returns
        -------
        measurement : `lsst.verify.Measurement`
            Measurement of the percentage of repeatability outliers, and associated metadata.
        """
        self.log.info("Measuring %s", metricName)
        pa2_thresh = self.config.threshPA2 * u.mmag

        # filter catalog        
        catalog = selectors.applySelectors(catalog,
                                           self.config.selectorActions,
                                           currentBands=currentBands)
        
        objectidColumn=self.config._getColumnName("objectID", currentBands)
        fluxColumn=self.config._getColumnName("flux", currentBands)
        fluxErrColumn=self.config._getColumnName("fluxErr", currentBands)
        
        # compute aggregated values
        photRepeatFrame = calcPhotRepeatTable(catalog,
                                            objectidColumn,
                                            fluxColumn,
                                            fluxErrColumn,
                                            residuals=True
                                            )

        okRms = (photRepeatFrame[("count")] > 2)
        okMeanSNR = (photRepeatFrame['meanSnr']> self.config.meanSNRCut)

        #compute PF1 metric
        import pdb; pdb.set_trace()
        if (("residuals" in photRepeatFrame.columns) and ((okRms & okMeanSNR).sum() > self.config.nMinPhotRepeat)):
            residualArray = (np.concatenate(
                    photRepeatFrame.loc[(okRms & okMeanSNR), "residuals"].values 
            ) *u.mag).to(u.mmag)
            percentileAtPA2 = (
                100 * np.mean(np.abs(residualArray.value) > pa2_thresh.value) * u.percent
            )          
            self.log.info("PF1 = %i percent" % percentileAtPA2.value)
            return Struct(measurement=Measurement("PF1", percentileAtPA2))
        else:
            return Struct(measurement=Measurement("PF1", np.nan * u.percent))