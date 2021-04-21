import numpy as np
import astropy.units as u

from lsst.faro.utils.filtermatches import filterMatches

__all__ = ("photRepeat", "calcPhotRepeat")


def photRepeat(matchedCatalog, nMinPhotRepeat=50, **filterargs):
    """Measure the photometric repeatability of a set of observations.

    Parameters
    ----------
    matchedCatalog : `lsst.afw.table.base.Catalog`
        `~lsst.afw.table.base.Catalog` object as created by
        `~lsst.afw.table.multiMatch` matching of sources from multiple visits.
    nMinPhotRepeat : `int`
        Minimum number of sources required to return a measurement.
    **filterargs
        Additional arguments to pass to `filterMatches` for catalog filtering.

    Returns
    -------
    photo_resid_meas : `dict`
        Photometric repeatability statistics and related quantities as measured
        by `calcPhotRepeat`. Returns NaN values if there are fewer than
        nMinPhotRepeat sources in the matched catalog.
    """
    filteredCat = filterMatches(matchedCatalog, **filterargs)
    magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

    # Require at least nMinPhotRepeat objects to calculate the repeatability:
    if filteredCat.count > nMinPhotRepeat:
        phot_resid_meas = calcPhotRepeat(filteredCat, magKey)
        # Check that the number of stars with >2 visits is >nMinPhotRepeat:
        okcount = (phot_resid_meas['count'] > 2)
        if np.sum(okcount) > nMinPhotRepeat:
            return phot_resid_meas
        else:
            return {'nomeas': np.nan*u.mmag}
    else:
        return {'nomeas': np.nan*u.mmag}


def calcPhotRepeat(matches, magKey):
    """Calculate the photometric repeatability of a set of measurements.

    Parameters
    ----------
    matches : `lsst.afw.table.GroupView`
        `~lsst.afw.table.GroupView` of sources matched between visits using
        MultiMatch, as provided by `lsst.faro.utils.matcher.match_catalogs`.
    magKey : `lsst.afw.table` schema key
        Magnitude column key in the ``GroupView``.
        E.g., ``magKey = allMatches.schema.find("slot_ModelFlux_mag").key``
        where ``allMatches`` is the result of `lsst.afw.table.MultiMatch.finish()`.

    Returns
    -------
    statistics : `dict`
        Repeatability statistics and ancillary quantities calculated from the
        input ``GroupView`` matched catalog. Fields are:
        - ``count``: array of number of nonzero magnitude measurements for each
          input source.
        - ``magMean``: `~astropy.unit.Quantity` array of mean magnitudes, in mag,
          for each input source.
        - ``rms``: `~astropy.unit.Quantity` array of RMS photometric repeatability
          about the mean, in mmag, for each input source.
        - ``repeatability``: scalar `~astropy.unit.Quantity` of the median ``rms``.
          This is calculated using all sources with more than 2 magnitude
          measurements, and reported in mmag.
        - ``magResid``: `~astropy.unit.Quantity` array for each input source,
          containing the magnitude residuals, in mmag, with respect to ``magMean``.
    """
    matches_rms = (matches.aggregate(np.nanstd, field=magKey)*u.mag).to(u.mmag)
    matches_count = matches.aggregate(np.count_nonzero, field=magKey)
    matches_mean = matches.aggregate(np.mean, field=magKey)*u.mag
    magResid = []
    for gp in matches.groups:
        magResid.append(((gp[magKey]-np.mean(gp[magKey]))*(u.mag)).to(u.mmag))
    magResid = np.array(magResid, dtype='object')
    okrms = (matches_count > 2)
    if np.sum(okrms) > 0:
        return {'count': matches_count, 'magMean': matches_mean, 'rms': matches_rms,
                'repeatability': np.median(matches_rms[okrms]), 'magResid': magResid}
    else:
        return {'count': 0, 'magMean': np.nan*u.mag, 'rms': np.nan*u.mmag,
                'repeatability': np.nan*u.mmag, 'magDiffs': 0*u.mmag}
