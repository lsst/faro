import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.afw.table import GroupView
from lsst.pex.config import Config, Field
from lsst.verify import Measurement, ThresholdSpecification
import lsst.pex.config as pexConfig
from sst_metrics_utils.filtermatches import filterMatches
from sst_metrics_utils.separations import calcRmsDistances, calcRmsDistancesVsRef
from lsst.validate.drp.repeatability import calcPhotRepeat
from lsst.validate.drp.calcsrd.tex import (correlation_function_ellipticity_from_matches,
                                           select_bin_from_corr)


filter_dict = {'u': 1, 'g': 2, 'r': 3, 'i': 4, 'z': 5, 'y': 6,
               'HSC-U': 1, 'HSC-G': 2, 'HSC-R': 3, 'HSC-I': 4, 'HSC-Z': 5, 'HSC-Y': 6}



class PA1TaskConfig(Config):
    brightSnrMin = Field(doc="Minimum median SNR for a source to be considered bright.",
                         dtype=float, default=50)
    brightSnrMax = Field(doc="Maximum median SNR for a source to be considered bright.",
                         dtype=float, default=np.Inf)


class PA1Task(Task):

    ConfigClass = PA1TaskConfig
    _DefaultName = "PA1Task"

    def __init__(self, config: PA1TaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.brightSnrMin = self.config.brightSnrMin
        self.brightSnrMax = self.config.brightSnrMax

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring PA1")

        filteredCat = filterMatches(matchedCatalog) #, extended=False, isPrimary=False)
        magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

        # Require at least nMinPA1=10 objects to calculate the repeatability:
        nMinPA1 = 50
        if filteredCat.count > nMinPA1:
            pa1 = calcPhotRepeat(filteredCat, magKey)
            return Struct(measurement=Measurement("PA1", pa1['repeatability']))
        else:
            return Struct(measurement=Measurement("PA1", np.nan*u.mmag))


class TExTaskConfig(Config):
    annulus_r = Field(doc="Radial size of the annulus in arcmin",
                      dtype=float, default=1.)
    comparison_operator = Field(doc="String representation of the operator to use in comparisons",
                                dtype=str, default="<=")


class TExTask(Task):
    ConfigClass = TExTaskConfig
    _DefaultName = "TExTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring {metric_name}")

        D = self.config.annulus_r * u.arcmin
        filteredCat = filterMatches(matchedCatalog)
        nMinTEx = 50
        if filteredCat.count <= nMinTEx:
            return Struct(measurement=Measurement(metric_name, np.nan*u.Unit('')))

        radius, xip, xip_err = correlation_function_ellipticity_from_matches(filteredCat)
        operator = ThresholdSpecification.convert_operator_str(self.config.comparison_operator)
        corr, corr_err = select_bin_from_corr(radius, xip, xip_err, radius=D, operator=operator)
        return Struct(measurement=Measurement(metric_name, np.abs(corr)*u.Unit('')))


class AMxTaskConfig(Config):
    annulus_r = Field(doc="Radial distance of the annulus in arcmin",
                      dtype=float, default=5.)
    width = Field(doc="Width of annulus in arcmin",
                  dtype=float, default=2.)
    bright_mag_cut = Field(doc="Bright limit of catalog entries to include",
                           dtype=float, default=17.0)
    faint_mag_cut = Field(doc="Faint limit of catalog entries to include",
                          dtype=float, default=21.5)


class AMxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "AMxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring {metric_name}")

        filteredCat = filterMatches(matchedCatalog)

        magRange = np.array([self.config.bright_mag_cut, self.config.faint_mag_cut]) * u.mag
        D = self.config.annulus_r * u.arcmin
        width = self.config.width * u.arcmin
        annulus = D + (width/2)*np.array([-1, +1])

        rmsDistances = calcRmsDistances(
            filteredCat,
            annulus,
            magRange=magRange)

        if len(rmsDistances) == 0:
            return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))

        return Struct(measurement=Measurement(metric_name, np.median(rmsDistances.to(u.marcsec))))


class AB1TaskConfig(Config):
    bright_mag_cut = Field(doc="Bright limit of catalog entries to include",
                           dtype=float, default=17.0)
    faint_mag_cut = Field(doc="Faint limit of catalog entries to include",
                          dtype=float, default=21.5)
    ref_filter = Field(doc="String representing the filter to use as reference",
                       dtype=str, default="r")


class AB1Task(Task):
    ConfigClass = AB1TaskConfig
    _DefaultName = "AB1Task"

    def run(self, matchedCatalogMulti, metric_name):
        self.log.info(f"Measuring {metric_name}")

        if self.config.ref_filter not in filter_dict:
            raise Exception('Reference filter supplied for AB1 not in dictionary.')

        filteredCat = filterMatches(matchedCatalogMulti)
        rmsDistancesAll = []

        if len(filteredCat) > 0:

            filtnum = filter_dict[self.config.ref_filter]

            refVisits = set()
            for id in filteredCat.ids:
                grptmp = filteredCat[id]
                filtmch = (grptmp['filt'] == filtnum)
                if len(filtmch) > 0:
                    refVisits.update(set(grptmp[filtmch]['visit']))

            refVisits = list(refVisits)

            magRange = np.array([self.config.bright_mag_cut, self.config.faint_mag_cut]) * u.mag

            for rv in refVisits:
                rmsDistances, distancesVisit = calcRmsDistancesVsRef(
                    filteredCat,
                    rv,
                    magRange=magRange)
                if len(rmsDistances) > 0:
                    rmsDistancesAll.append(rmsDistances)

            rmsDistancesAll = np.array(rmsDistancesAll)

            if len(rmsDistancesAll) == 0:
                return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))
            return Struct(measurement=Measurement(metric_name, np.nanmean(rmsDistancesAll)*u.marcsec))

        else:
            return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))
