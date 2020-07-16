import numpy as np
import astropy.units as u
from metric_pipeline_utils.filtermatches import filterMatches
from lsst.validate.drp.repeatability import calcPhotRepeat


def photRepeat(matchedCatalog, **filterargs):
    filteredCat = filterMatches(matchedCatalog, **filterargs) #, extended=False, isPrimary=False)
    magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

    # Require at least nMinPhotRepeat objects to calculate the repeatability:
    nMinPhotRepeat = 50
    if filteredCat.count > nMinPhotRepeat:
        phot_resid_meas = calcPhotRepeat(filteredCat, magKey)
        return phot_resid_meas
    else:
        return {'nomeas': np.nan*u.mmag}
