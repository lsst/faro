import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import ChoiceField, Config, Field, ListField
from lsst.verify import Measurement, Datum
from lsst.faro.utils.filtermatches import filterMatches
from lsst.faro.utils.separations import (calcRmsDistances, calcRmsDistancesVsRef,
                                         astromResiduals)
from lsst.faro.utils.phot_repeat import photRepeat


__all__ = ("PA1TaskConfig", "PA1Task", "PF1TaskConfig", "PF1Task",
           "AMxTaskConfig", "AMxTask", "ADxTask", "AFxTask", "AB1TaskConfig", "AB1Task", "ModelPhotRepTask")


filter_dict = {'u': 1, 'g': 2, 'r': 3, 'i': 4, 'z': 5, 'y': 6,
               'HSC-U': 1, 'HSC-G': 2, 'HSC-R': 3, 'HSC-I': 4, 'HSC-Z': 5, 'HSC-Y': 6}


class PA1TaskConfig(Config):
    """Config fields for the PA1 photometric repeatability metric.
    """
    brightSnrMin = Field(doc="Minimum median SNR for a source to be considered bright.",
                         dtype=float, default=200)
    brightSnrMax = Field(doc="Maximum median SNR for a source to be considered bright.",
                         dtype=float, default=np.Inf)
    nMinPhotRepeat = Field(doc="Minimum number of objects required for photometric repeatability.",
                           dtype=int, default=50)
    writeExtras = Field(doc="Write out the magnitude residuals and rms values for debugging.",
                        dtype=bool, default=False)


class PA1Task(Task):
    """A Task that computes the PA1 photometric repeatability metric from an
    input set of multiple visits of the same field.

    Notes
    -----
    The intended usage is to retarget the run method of
    `lsst.faro.measurement.TractMatchedMeasurementTask` to PA1Task.
    This metric is calculated on a set of matched visits, and aggregated at the tract level.
    """

    ConfigClass = PA1TaskConfig
    _DefaultName = "PA1Task"

    def __init__(self, config: PA1TaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)

    def run(self, matchedCatalog, metricName):
        """Calculate the photometric repeatability.

        Parameters
        ----------
        matchedCatalog : `lsst.afw.table.base.Catalog`
            `~lsst.afw.table.base.Catalog` object as created by
            `~lsst.afw.table.multiMatch` matching of sources from multiple visits.
        metricName : `str`
            The name of the metric.

        Returns
        -------
        measurement : `lsst.verify.Measurement`
            Measurement of the repeatability and its associated metadata.
        """
        self.log.info("Measuring %s", metricName)

        pa1 = photRepeat(matchedCatalog, nMinPhotRepeat=self.config.nMinPhotRepeat,
                         snrMax=self.config.brightSnrMax, snrMin=self.config.brightSnrMin)

        if 'magMean' in pa1.keys():
            if self.config.writeExtras:
                extras = {
                    'rms': Datum(pa1['rms'], label='RMS',
                                 description='Photometric repeatability rms for each star.'),
                    'count': Datum(pa1['count']*u.count, label='count',
                                   description='Number of detections used to calculate repeatability.'),
                    'mean_mag': Datum(pa1['magMean'], label='mean_mag',
                                      description='Mean magnitude of each star.'),
                }
                return Struct(measurement=Measurement("PA1", pa1['repeatability'], extras=extras))
            else:
                return Struct(measurement=Measurement("PA1", pa1['repeatability']))
        else:
            return Struct(measurement=Measurement("PA1", np.nan*u.mmag))


class PF1TaskConfig(Config):
    brightSnrMin = Field(doc="Minimum median SNR for a source to be considered bright.",
                         dtype=float, default=200)
    brightSnrMax = Field(doc="Maximum median SNR for a source to be considered bright.",
                         dtype=float, default=np.Inf)
    nMinPhotRepeat = Field(doc="Minimum number of objects required for photometric repeatability.",
                           dtype=int, default=50)
    # The defaults for threshPA2 correspond to the SRD "design" thresholds.
    threshPA2 = Field(doc="Threshold in mmag for PF1 calculation.", dtype=float, default=15.0)


class PF1Task(Task):
    """A Task that computes PF1, the percentage of photometric repeatability measurements
    that deviate by more than PA2 mmag from the mean.

    Notes
    -----
    The intended usage is to retarget the run method of
    `lsst.faro.measurement.TractMatchedMeasurementTask` to PF1Task. This Task uses the
    same set of photometric residuals that are calculated for the PA1 metric.
    This metric is calculated on a set of matched visits, and aggregated at the tract level.
    """
    ConfigClass = PF1TaskConfig
    _DefaultName = "PF1Task"

    def __init__(self, config: PF1TaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)

    def run(self, matchedCatalog, metricName):
        """Calculate the percentage of outliers in the photometric repeatability values.

        Parameters
        ----------
        matchedCatalog : `lsst.afw.table.base.Catalog`
            `~lsst.afw.table.base.Catalog` object as created by
            `~lsst.afw.table.multiMatch` matching of sources from multiple visits.
        metricName : `str`
            The name of the metric.

        Returns
        -------
        measurement : `lsst.verify.Measurement`
            Measurement of the percentage of repeatability outliers, and associated metadata.
        """
        self.log.info("Measuring %s", metricName)
        pa2_thresh = self.config.threshPA2 * u.mmag

        pf1 = photRepeat(matchedCatalog, nMinPhotRepeat=self.config.nMinPhotRepeat,
                         snrMax=self.config.brightSnrMax, snrMin=self.config.brightSnrMin)

        if 'magResid' in pf1.keys():
            # Previously, validate_drp used the first random sample from PA1 measurement
            # Now, use all of them.
            # Keep only stars with > 2 observations:
            okrms = (pf1['count'] > 2)
            magResid0 = pf1['magResid']
            magResid = np.concatenate(magResid0[okrms])

            percentileAtPA2 = 100 * np.mean(np.abs(magResid.value) > pa2_thresh.value) * u.percent

            return Struct(measurement=Measurement("PF1", percentileAtPA2))
        else:
            return Struct(measurement=Measurement("PF1", np.nan*u.percent))


def isSorted(a):
    return all(a[i] <= a[i+1] for i in range(len(a)-1))


def bins(window, n):
    delta = window/n
    return [i*delta for i in range(n+1)]


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
    bins = ListField(doc="Bins for histogram.",
                     dtype=float, minLength=2, maxLength=1500,
                     listCheck=isSorted, default=bins(30, 200))


class AMxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "AMxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info("Measuring %s", metric_name)

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

        return Struct(measurement=Measurement(metric_name, np.median(rmsDistances.to(u.marcsec)),
                                              extras=extras))


class ADxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "ADxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info("Measuring %s", metric_name)

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
            absDiffsMarcsec = (sepDistances-np.median(sepDistances)).to(u.marcsec)
            return Struct(measurement=Measurement(metric_name, np.percentile(absDiffsMarcsec.value,
                          afPercentile.value)*u.marcsec))


class AFxTask(Task):
    ConfigClass = AMxTaskConfig
    _DefaultName = "AFxTask"

    def run(self, matchedCatalog, metric_name):
        self.log.info("Measuring %s", metric_name)

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
            absDiffsMarcsec = (sepDistances-np.median(sepDistances)).to(u.marcsec)
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

    def run(self, matchedCatalogMulti, metric_name, in_id, out_id):
        self.log.info("Measuring %s", metric_name)

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
                rmsDistances = calcRmsDistancesVsRef(
                    filteredCat,
                    rv,
                    magRange=magRange,
                    band=filter_dict[out_id['band']])
                finiteEntries = np.where(np.isfinite(rmsDistances))[0]
                if len(finiteEntries) > 0:
                    rmsDistancesAll.append(rmsDistances[finiteEntries])

            if len(rmsDistancesAll) == 0:
                return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))
            else:
                rmsDistancesAll = np.concatenate(rmsDistancesAll)
                return Struct(measurement=Measurement(metric_name, np.mean(rmsDistancesAll)))

        else:
            return Struct(measurement=Measurement(metric_name, np.nan*u.marcsec))


class ModelPhotRepTaskConfig(Config):
    """Config fields for the *ModelPhotRep photometric repeatability metrics.
    """
    index = ChoiceField(doc="Index of the metric definition", dtype=int,
                        allowed={x: f"Nth (N={x}) lowest S/N bin" for x in range(1, 5)},
                        optional=False)
    magName = Field(doc="Name of the magnitude column", dtype=str, default="slot_ModelFlux_mag")
    nMinPhotRepeat = Field(doc="Minimum number of objects required for photometric repeatability.",
                           dtype=int, default=50)
    prefix = Field(doc="Prefix of the metric name", dtype=str, default="model")
    selectExtended = Field(doc="Whether to select extended sources", dtype=bool)
    selectSnrMin = Field(doc="Minimum median SNR for a source to be selected.",
                         dtype=float)
    selectSnrMax = Field(doc="Maximum median SNR for a source to be selected.",
                         dtype=float)
    writeExtras = Field(doc="Write out the magnitude residuals and rms values for debugging.",
                        dtype=bool, default=False)


class ModelPhotRepTask(Task):
    """A Task that computes a *ModelPhotRep photometric repeatability metric
     from an input set of multiple visits of the same field.

    Notes
    -----
    The intended usage is to retarget the run method of
    `lsst.faro.measurement.TractMatchedMeasurementTask` to ModelPhotRepTask.
    This metric is calculated on a set of matched visits, and aggregated at the tract level.
    """

    ConfigClass = ModelPhotRepTaskConfig
    _DefaultName = "ModelPhotRepTask"

    def __init__(self, config: ModelPhotRepTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)

    def run(self, matchedCatalog, metricName):
        """Calculate the photometric repeatability.

        Parameters
        ----------
        matchedCatalog : `lsst.afw.table.base.Catalog`
            `~lsst.afw.table.base.Catalog` object as created by
            `~lsst.afw.table.multiMatch` matching of sources from multiple visits.
        metricName : `str`
            The name of the metric.

        Returns
        -------
        measurement : `lsst.verify.Measurement`
            Measurement of the repeatability and its associated metadata.
        """
        self.log.info("Measuring %s", metricName)

        meas = photRepeat(matchedCatalog, nMinPhotRepeat=self.config.nMinPhotRepeat,
                          snrMax=self.config.selectSnrMax, snrMin=self.config.selectSnrMin,
                          magName=self.config.magName, extended=self.config.selectExtended)

        name_type = "Gal" if self.config.selectExtended else "Star"
        name_meas = f'{self.config.prefix}PhotRep{name_type}{self.config.index}'

        if 'magMean' in meas.keys():
            if self.config.writeExtras:
                extras = {
                    'rms': Datum(meas['rms'], label='RMS',
                                 description='Photometric repeatability rms for each star.'),
                    'count': Datum(meas['count']*u.count, label='count',
                                   description='Number of detections used to calculate repeatability.'),
                    'mean_mag': Datum(meas['magMean'], label='mean_mag',
                                      description='Mean magnitude of each star.'),
                }
                return Struct(measurement=Measurement(name_meas, meas['repeatability'], extras=extras))
            else:
                return Struct(measurement=Measurement(name_meas, meas['repeatability']))
        else:
            return Struct(measurement=Measurement(name_meas, np.nan*u.mmag))
