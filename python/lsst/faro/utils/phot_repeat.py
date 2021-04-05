# import functools
# import math
import numpy as np
# from scipy.stats import norm
import astropy.units as u

# import lsst.pipe.base as pipeBase
from lsst.faro.utils.filtermatches import filterMatches

__all__ = ("photRepeat", "calcPhotRepeat", "cal_RMS")


def photRepeat(matchedCatalog, nMinPhotRepeat=50, **filterargs):
    filteredCat = filterMatches(matchedCatalog, **filterargs)
    magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

    # Require at least nMinPhotRepeat objects to calculate the repeatability:
    if filteredCat.count > nMinPhotRepeat:
        phot_resid_meas = calcPhotRepeat(filteredCat, magKey)
        return phot_resid_meas
    else:
        return {'nomeas': np.nan*u.mmag}


def cal_RMS(x):
    return np.sqrt(np.mean((x-np.mean(x))**2))


def calcPhotRepeat(matches, magKey):
    matches_rms = matches.aggregate(cal_RMS, field=magKey)*1000.0*u.mmag
    matches_count = matches.aggregate(np.count_nonzero, field=magKey)
    matches_mean = matches.aggregate(np.mean, field=magKey)*u.mag
    magDiff = []
    for gp in matches.groups:
        magDiff.append((gp[magKey]-np.mean(gp[magKey]))*1000.0*u.mmag)
    # magDiffs = np.array([gp[magKey]-np.mean(gp[magKey]) for gp in matches.groups]) * 1000.0 * u.mmag
    magDiffs = np.array(magDiff, dtype='object')
    # magDiffs = np.concatenate(magDiff)
    # import pdb; pdb.set_trace()
    okrms = (matches_count > 2)
    if np.sum(okrms) > 0:
        return {'count': matches_count, 'magMean': matches_mean, 'rms': matches_rms,
                'repeatability': np.median(matches_rms[okrms]), 'magDiffs': magDiffs}
    else:
        return {'count': 0, 'magMean': np.nan*u.mag, 'rms': np.nan*u.mmag,
                'repeatability': np.nan*u.mmag, 'magDiffs': 0*u.mmag}


'''
def calcPhotRepeat(matches, magKey, numRandomShuffles=50, randomSeed=None):
    """Calculate the photometric repeatability of measurements across a set
    of randomly selected pairs of visits.
    Parameters
    ----------
    matches : `lsst.afw.table.GroupView`
        `~lsst.afw.table.GroupView` of sources matched between visits,
        from MultiMatch, provided by
        `lsst.faro.utils.matcher.match_catalogs`.
    magKey : `lsst.afw.table` schema key
        Magnitude column key in the ``groupView``.
        E.g., ``magKey = allMatches.schema.find("slot_ModelFlux_mag").key``
        where ``allMatches`` is the result of
        `lsst.afw.table.MultiMatch.finish()`.
    numRandomShuffles : int
        Number of times to draw random pairs from the different observations.
    randomSeed : int
        Seed for random number generation when choosing samples.
    Returns
    -------
    statistics : `dict`
        Statistics to compute model_phot_rep. Fields are:
        - ``model_phot_rep``: scalar `~astropy.unit.Quantity` of mean ``iqr``.
          This is formally the model_phot_rep metric measurement.
        - ``rms``: `~astropy.unit.Quantity` array in mmag of photometric
          repeatability RMS across ``numRandomShuffles``.
          Shape: ``(nRandomSamples,)``.
        - ``iqr``: `~astropy.unit.Quantity` array in mmag of inter-quartile
          range of photometric repeatability distribution.
          Shape: ``(nRandomSamples,)``.
        - ``magDiff``: `~astropy.unit.Quantity` array of magnitude differences
          between pairs of sources. Shape: ``(nRandomSamples, nMatches)``.
        - ``magMean``: `~astropy.unit.Quantity` array of mean magnitudes of
          each pair of sources. Shape: ``(nRandomSamples, nMatches)``.
    Notes
    -----
    We calculate differences for ``numRandomShuffles`` different random
    realizations of the measurement pairs, to provide some estimate of the
    uncertainty on our RMS estimates due to the random shuffling.  This
    estimate could be stated and calculated from a more formally derived
    motivation but in practice 50 should be sufficient.
    The LSST Science Requirements Document (LPM-17), or SRD, characterizes the
    photometric repeatability by putting a requirement on the median RMS of
    measurements of non-variable bright stars.  This quantity is PA1, with a
    design, minimum, and stretch goals of (5, 8, 3) millimag following LPM-17
    as of 2011-07-06, available at http://ls.st/LPM-17. model_phot_rep is a
    similar quantity measured for extended sources (almost entirely galaxies),
    for which no requirement currently exists in the SRD.
    This present routine calculates this quantity in two different ways:
    1. RMS
    2. interquartile range (IQR)
    **The repeatability scalar measurement is the median of the IQR.**
    This function also returns additional quantities of interest:
    - the pair differences of observations of sources,
    - the mean magnitude of each source
    Examples
    --------
    Normally ``calcPhotRepeat`` is called by `measurePhotRepeat`, using
    data from `metric_pipeline_utils.matcher.match_catalogs`. Here's an
    example of how to call ``calcPhotRepeat`` directly given the Butler output
    repository generated by examples/runHscQuickTest.sh:
    >>> import lsst.daf.persistence as dafPersist
    >>> from lsst.afw.table import SourceCatalog, SchemaMapper, Field
    >>> from lsst.afw.table import MultiMatch, SourceRecord, GroupView
    >>> from from metric_pipeline_utils.phot_repeat import calcPhotRepeat
    >>> from lsst.validate.drp.util import discoverDataIds
    >>> import numpy as np
    >>> repo = 'HscQuick/output'
    >>> butler = dafPersist.Butler(repo)
    >>> dataset = 'src'
    >>> schema = butler.get(dataset + '_schema', immediate=True).schema
    >>> visitDataIds = discoverDataIds(repo)
    >>> mmatch = None
    >>> for vId in visitDataIds:
    ...     cat = butler.get('src', vId)
    ...     calib = butler.get('calexp_photoCalib', vId)
    ...     cat = calib.calibrateCatalog(cat, ['modelfit_CModel'])
    ...     if mmatch is None:
    ...         mmatch = MultiMatch(cat.schema,
    ...                             dataIdFormat={'visit': np.int32, 'ccd': np.int32},
    ...                             RecordClass=SourceRecord)
    ...     mmatch.add(catalog=cat, dataId=vId)
    ...
    >>> matchCat = mmatch.finish()
    >>> allMatches = GroupView.build(matchCat)
    >>> magKey = allMatches.schema.find('slot_ModelFlux_mag').key
    >>> def matchFilter(cat):
    >>>     if len(cat) < 2:
    >>>         return False
    >>>     return np.isfinite(cat.get(magKey)).all()
    >>> repeat = calcPhotRepeat(allMatches.where(matchFilter), magKey)
    """
    rng = np.random.default_rng(randomSeed)
    mprSamples = [calcPhotRepeatSample(matches, magKey, rng=rng)
                  for _ in range(numRandomShuffles)]

    rms = np.array([mpr.rms for mpr in mprSamples]) * u.mmag
    iqr = np.array([mpr.iqr for mpr in mprSamples]) * u.mmag
    magDiff = np.array([mpr.magDiffs for mpr in mprSamples]) * u.mmag
    magMean = np.array([mpr.magMean for mpr in mprSamples]) * u.mag
    repeat = np.mean(iqr)
    return {'rms': rms, 'iqr': iqr, 'magDiff': magDiff, 'magMean': magMean, 'repeatability': repeat}


def calcPhotRepeatSample(matches, magKey, rng=None):
    """Compute one realization of repeatability by randomly sampling pairs of
    visits.
    Parameters
    ----------
    matches : `lsst.afw.table.GroupView`
        `~lsst.afw.table.GroupView` of sources matched between visits,
        from MultiMatch, provided by
        `lsst.faro.utils.matcher.match_catalogs`.
    magKey : `lsst.afw.table` schema key
        Magnitude column key in the ``groupView``.
        E.g., ``magKey = allMatches.schema.find("base_PsfFlux_mag").key``
        where ``allMatches`` is the result of
        `lsst.afw.table.MultiMatch.finish()`.
    rng : `numpy.random._generator.Generator`
        Input random number generator.
    Returns
    -------
    metrics : `lsst.pipe.base.Struct`
        Metrics of pairs of sources matched between two visits. Fields are:
        - ``rms``: scalar RMS of differences of sources observed in this
          randomly sampled pair of visits.
        - ``iqr``: scalar inter-quartile range (IQR) of differences of sources
          observed in a randomly sampled pair of visits.
        - ``magDiffs`: array, shape ``(nMatches,)``, of magnitude differences
          (mmag) for observed sources across a randomly sampled pair of visits.
        - ``magMean``: array, shape ``(nMatches,)``, of mean magnitudes
          of sources observed across a randomly sampled pair of visits.
    See also
    --------
    calcPhotRepeat : A wrapper that repeatedly calls this function to build
        the repeatability measurement.
    """
    sampler = functools.partial(getRandomDiffRmsInMmags, rng=rng)
    magDiffs = matches.aggregate(sampler, field=magKey)
    magMean = matches.aggregate(np.mean, field=magKey)
    rms, iqr = computeWidths(magDiffs)
    return pipeBase.Struct(rms=rms, iqr=iqr, magDiffs=magDiffs, magMean=magMean,)


def computeWidths(array):
    """Compute the RMS and the scaled inter-quartile range of an array.
    Parameters
    ----------
    array : `list` or `numpy.ndarray`
        Array.
    Returns
    -------
    rms : `float`
        RMS
    iqr : `float`
        Scaled inter-quartile range (IQR, see *Notes*).
    Notes
    -----
    We estimate the width of the histogram in two ways:
    - using a simple RMS,
    - using the interquartile range (IQR)
    The IQR is scaled by the IQR/RMS ratio for a Gaussian such that it
    if the array is Gaussian distributed, then the scaled IQR = RMS.
    """
    # For scalars, math.sqrt is several times faster than numpy.sqrt.
    rmsSigma = math.sqrt(np.mean(array**2))
    iqrSigma = np.subtract.reduce(np.percentile(array, [75, 25])) / (norm.ppf(0.75)*2)
    return rmsSigma, iqrSigma


def getRandomDiffRmsInMmags(array, rng=None):
    """Calculate the RMS difference in mmag between a random pairing of
    visits of a source.
    Parameters
    ----------
    array : `list` or `numpy.ndarray`
        Magnitudes from which to select the pair [mag].
    rng : `numpy.random._generator.Generator`
        Input random number generator.
    Returns
    -------
    rmsMmags : `float`
        RMS difference in mmag from a random pair of visits.
    Notes
    -----
    The LSST SRD recommends computing repeatability from a histogram of
    magnitude differences for the same source measured on two visits
    (using a median over the magDiffs to reject outliers).
    Because we have N>=2 measurements for each source, we select a random
    pair of visits for each source.  We divide each difference by sqrt(2)
    to obtain the RMS about the (unknown) mean magnitude,
    instead of obtaining just the RMS difference.
    See Also
    --------
    getRandomDiff : Get the difference between two randomly selected elements of an array.
    Examples
    --------
    >>> mag = [24.2, 25.5]
    >>> rms = getRandomDiffRmsInMmags(mag)
    >>> print(rms)
    212.132034
    """
    thousandDivSqrtTwo = 1000/math.sqrt(2)
    return thousandDivSqrtTwo * getRandomDiff(array, rng=rng)


def getRandomDiff(array, rng=None):
    """Get the difference between two randomly selected elements of an array.
    Parameters
    ----------
    array : `list` or `numpy.ndarray`
        Input array.
    rng : `numpy.random._generator.Generator`
        Input random number generator.
    Returns
    -------
    float or int
        Difference between two random elements of the array.
    """
    if not rng:
        rng = np.random.default_rng()
    a, b = rng.choice(range(len(array)), 2, replace=False)
    return array[a] - array[b]
'''
