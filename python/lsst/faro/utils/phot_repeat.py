import numpy as np
import astropy.units as u

from lsst.faro.utils.filtermatches import filterMatches

__all__ = ("photRepeat", "calcPhotRepeat")


def photRepeat(matchedCatalog, nMinPhotRepeat=50, **filterargs):
    filteredCat = filterMatches(matchedCatalog, **filterargs)
    magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

    # Require at least nMinPhotRepeat objects to calculate the repeatability:
    if filteredCat.count > nMinPhotRepeat:
        phot_resid_meas = calcPhotRepeat(filteredCat, magKey)
        # Check that the number of stars with >2 visits is >nMinPhotRepeat:
        okcount = (phot_resid_meas['count'] > 2)
        if okcount > nMinPhotRepeat:
            return phot_resid_meas
        else:
            return {'nomeas': np.nan*u.mmag}
    else:
        return {'nomeas': np.nan*u.mmag}


def calcPhotRepeat(matches, magKey):
    matches_rms = matches.aggregate(np.nanstd, field=magKey)*(u.mag).to(u.mmag)
    matches_count = matches.aggregate(np.count_nonzero, field=magKey)
    matches_mean = matches.aggregate(np.mean, field=magKey)*u.mag
    magResid = []
    for gp in matches.groups:
        magResid.append((gp[magKey]-np.mean(gp[magKey]))*(u.mag).to(u.mmag))
    magResid = np.array(magResid, dtype='object')
    okrms = (matches_count > 2)
    if np.sum(okrms) > 0:
        return {'count': matches_count, 'magMean': matches_mean, 'rms': matches_rms,
                'repeatability': np.median(matches_rms[okrms]), 'magResid': magResid}
    else:
        return {'count': 0, 'magMean': np.nan*u.mag, 'rms': np.nan*u.mmag,
                'repeatability': np.nan*u.mmag, 'magDiffs': 0*u.mmag}
