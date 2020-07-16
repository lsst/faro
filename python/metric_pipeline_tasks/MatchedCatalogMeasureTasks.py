import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config, Field, ListField
from lsst.verify import Measurement, ThresholdSpecification, Datum
from metric_pipeline_utils.filtermatches import filterMatches
from metric_pipeline_utils.separations import (calcRmsDistances, calcRmsDistancesVsRef,
                                               astromRms, astromResiduals)
from metric_pipeline_utils.phot_repeat import photRepeat
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

        pa1 = photRepeat(matchedCatalog, snrMax=self.brightSnrMax, snrMin=self.brightSnrMin)

        if 'magDiff' in pa1.keys():
            return Struct(measurement=Measurement("PA1", pa1['repeatability']))
        else:
            return Struct(measurement=Measurement("PA1", np.nan*u.mmag))


class PA2TaskConfig(Config):
    brightSnrMin = Field(doc="Minimum median SNR for a source to be considered bright.",
                         dtype=float, default=50)
    brightSnrMax = Field(doc="Maximum median SNR for a source to be considered bright.",
                         dtype=float, default=np.Inf)
    # The defaults for threshPA2 and threshPF1 correspond to the SRD "design" thresholds.
    threshPA2 = Field(doc="Threshold in mmag for PF1 calculation.", dtype=float, default=15.0)
    threshPF1 = Field(doc="Percentile of differences that can vary by more than threshPA2.",
                      dtype=float, default=10.0)


class PA2Task(Task):

    ConfigClass = PA2TaskConfig
    _DefaultName = "PA2Task"

    def __init__(self, config: PA2TaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.brightSnrMin = self.config.brightSnrMin
        self.brightSnrMax = self.config.brightSnrMax
        self.threshPA2 = self.config.threshPA2
        self.threshPF1 = self.config.threshPF1

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring PA2")
        pf1_thresh = self.threshPF1 * u.percent

        pa2 = photRepeat(matchedCatalog)

        if 'magDiff' in pa2.keys():
            # Previously, validate_drp used the first random sample from PA1 measurement
            # Now, use all of them.
            magDiffs = pa2['magDiff']

            pf1Percentile = 100.*u.percent - pf1_thresh
            return Struct(measurement=Measurement("PA2", np.percentile(np.abs(magDiffs.value),
                          pf1Percentile.value) * magDiffs.unit))
        else:
            return Struct(measurement=Measurement("PA2", np.nan*u.mmag))


class PF1Task(Task):

    ConfigClass = PA2TaskConfig
    _DefaultName = "PF1Task"

    def __init__(self, config: PA2TaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.brightSnrMin = self.config.brightSnrMin
        self.brightSnrMax = self.config.brightSnrMax
        self.threshPA2 = self.config.threshPA2
        self.threshPF1 = self.config.threshPF1

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring PF1")
        pa2_thresh = self.threshPA2 * u.mmag

        pf1 = photRepeat(matchedCatalog)

        if 'magDiff' in pf1.keys():
            # Previously, validate_drp used the first random sample from PA1 measurement
            # Now, use all of them.
            magDiffs = pf1['magDiff']

            percentileAtPA2 = 100 * np.mean(np.abs(magDiffs.value) > pa2_thresh.value) * u.percent

            return Struct(measurement=Measurement("PF1", percentileAtPA2))
        else:
            return Struct(measurement=Measurement("PF1", np.nan*u.percent))


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
    annulus_r = Field(doc="Radial distance of the annulus in arcmin (5, 20, or 200 for AM1, AM2, AM3)",
                      dtype=float, default=5.)
    width = Field(doc="Width of annulus in arcmin",
                  dtype=float, default=2.)
    bright_mag_cut = Field(doc="Bright limit of catalog entries to include",
                           dtype=float, default=17.0)
    faint_mag_cut = Field(doc="Faint limit of catalog entries to include",
                          dtype=float, default=21.5)
    # The defaults for threshADx and threshAFx correspond to the SRD "design" thresholds.
    threshAD = Field(doc="Threshold in mas for AFx calculation.", dtype=float, default=20.0)
    threshAF = Field(doc="Percentile of differences that can vary by more than threshAD.",
                     dtype=float, default=10.0)


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


class ADxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "ADxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring {metric_name}")

        sepDistances = astromResiduals(matchedCatalog, self.config.bright_mag_cut,
                                       self.config.faint_mag_cut, self.config.annulus_r,
                                       self.config.width)

        afThresh = self.config.threshAF * u.percent
        afPercentile = 100.0*u.percent - afThresh

        if len(sepDistances) <= 1:
            return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))
        else:
            # absolute value of the difference between each astrometric rms
            #    and the median astrometric RMS
            # absRmsDiffs = np.abs(rmsDistances - np.median(rmsDistances)).to(u.marcsec)
            absDiffsMarcsec = sepDistances.to(u.marcsec)
            return Struct(measurement=Measurement(metric_name, np.percentile(absDiffsMarcsec.value,
                          afPercentile.value)*u.marcsec))


=======
def isSorted(l):
    return all(l[i] <= l[i+1] for i in range(len(l)-1))


def bins(window, n):
    delta = window/n
    return [i*delta for i in range(n+1)]


class AMxWithHistTaskConfig(AMxTaskConfig):
    bins = ListField(doc="Bins for histogram.",
                     dtype=float, minLength=2, maxLength=1500,
                     listCheck=isSorted, default=bins(30, 200))



class AMxWithHistTask(Task):
    ConfigClass = AMxWithHistTaskConfig
    _DefaultName = "AMxWithHistTask"

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

        values, bins = np.histogram(rmsDistances.to(u.marcsec), bins=self.config.bins*u.marcsec)
        extras = {'bins': Datum(bins, label='binvalues', description='bins'),
                  'values': Datum(values*u.count, label='counts', description='icounts in bins')}

        if len(rmsDistances) == 0:
            return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec, extras=extras))

        return Struct(measurement=Measurement(metric_name, np.median(rmsDistances.to(u.marcsec)), extras=extras))


class AFxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "AFxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info(f"Measuring {metric_name}")

        sepDistances = astromResiduals(matchedCatalog, self.config.bright_mag_cut,
                                       self.config.faint_mag_cut, self.config.annulus_r,
                                       self.config.width)

        adxThresh = self.config.threshAD * u.marcsec

        if len(sepDistances) <= 1:
            return Struct(measurement=Measurement(metric_name, np.nan*u.percent))
        else:
            # absolute value of the difference between each astrometric rms
            #    and the median astrometric RMS
            # absRmsDiffs = np.abs(rmsDistances - np.median(rmsDistances)).to(u.marcsec)
            absDiffsMarcsec = sepDistances.to(u.marcsec)
            percentileAtADx = 100 * np.mean(np.abs(absDiffsMarcsec.value) > adxThresh.value) * u.percent
            return Struct(measurement=Measurement(metric_name, percentileAtADx))


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
