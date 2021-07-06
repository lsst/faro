import numpy as np
import astropy.units as u
import logging
import lsst.geom as geom
from lsst.faro.utils.filtermatches import filterMatches
from lsst.faro.utils.coord_util import (averageRaFromCat, averageDecFromCat,
                                        sphDist)

__all__ = ("astromRms", "astromResiduals", "calcRmsDistances", "calcSepOutliers",
           "matchVisitComputeDistance", "calcRmsDistancesVsRef")


def astromRms(matchedCatalog, mag_bright_cut, mag_faint_cut, annulus_r, width, **filterargs):
    filteredCat = filterMatches(matchedCatalog, **filterargs)

    magRange = np.array([mag_bright_cut, mag_faint_cut]) * u.mag
    D = annulus_r * u.arcmin
    width = width * u.arcmin
    annulus = D + (width/2)*np.array([-1, +1])

    # Require at least 2 measurements to calculate the repeatability:
    nMinMeas = 2
    if filteredCat.count > nMinMeas:
        astrom_resid_rms_meas = calcRmsDistances(
            filteredCat,
            annulus,
            magRange=magRange)
        return astrom_resid_rms_meas
    else:
        return {'nomeas': np.nan*u.marcsec}


def astromResiduals(matchedCatalog, mag_bright_cut, mag_faint_cut, annulus_r, width, **filterargs):
    filteredCat = filterMatches(matchedCatalog, **filterargs)

    magRange = np.array([mag_bright_cut, mag_faint_cut]) * u.mag
    D = annulus_r * u.arcmin
    width = width * u.arcmin
    annulus = D + (width/2)*np.array([-1, +1])

    # Require at least 2 measurements to calculate the repeatability:
    nMinMeas = 2
    if filteredCat.count > nMinMeas:
        astrom_resid_meas = calcSepOutliers(
            filteredCat,
            annulus,
            magRange=magRange)
        return astrom_resid_meas
    else:
        return {'nomeas': np.nan*u.marcsec}


def calcRmsDistances(groupView, annulus, magRange, verbose=False):
    """Calculate the RMS distance of a set of matched objects over visits.
    Parameters
    ----------
    groupView : lsst.afw.table.GroupView
        GroupView object of matched observations from MultiMatch.
    annulus : length-2 `astropy.units.Quantity`
        Distance range (i.e., arcmin) in which to compare objects.
        E.g., `annulus=np.array([19, 21]) * u.arcmin` would consider all
        objects separated from each other between 19 and 21 arcminutes.
    magRange : length-2 `astropy.units.Quantity`
        Magnitude range from which to select objects.
    verbose : bool, optional
        Output additional information on the analysis steps.
    Returns
    -------
    rmsDistances : `astropy.units.Quantity`
        RMS angular separations of a set of matched objects over visits.
    """
    log = logging.getLogger(__name__)

    # First we make a list of the keys that we want the fields for
    importantKeys = [groupView.schema.find(name).key for
                     name in ['id', 'coord_ra', 'coord_dec',
                              'object', 'visit', 'base_PsfFlux_mag']]

    minMag, maxMag = magRange.to(u.mag).value

    def magInRange(cat):
        mag = cat.get('base_PsfFlux_mag')
        w, = np.where(np.isfinite(mag))
        medianMag = np.median(mag[w])
        return minMag <= medianMag and medianMag < maxMag

    groupViewInMagRange = groupView.where(magInRange)

    # List of lists of id, importantValue
    matchKeyOutput = [obj.get(key)
                      for key in importantKeys
                      for obj in groupViewInMagRange.groups]

    jump = len(groupViewInMagRange)

    ra = matchKeyOutput[1*jump:2*jump]
    dec = matchKeyOutput[2*jump:3*jump]
    visit = matchKeyOutput[4*jump:5*jump]

    # Calculate the mean position of each object from its constituent visits
    # `aggregate` calulates a quantity for each object in the groupView.
    meanRa = groupViewInMagRange.aggregate(averageRaFromCat)
    meanDec = groupViewInMagRange.aggregate(averageDecFromCat)

    annulusRadians = arcminToRadians(annulus.to(u.arcmin).value)

    rmsDistances = list()
    for obj1, (ra1, dec1, visit1) in enumerate(zip(meanRa, meanDec, visit)):
        dist = sphDist(ra1, dec1, meanRa[obj1+1:], meanDec[obj1+1:])
        objectsInAnnulus, = np.where((annulusRadians[0] <= dist)
                                     & (dist < annulusRadians[1]))
        objectsInAnnulus += obj1 + 1
        for obj2 in objectsInAnnulus:
            distances = matchVisitComputeDistance(
                visit[obj1], ra[obj1], dec[obj1],
                visit[obj2], ra[obj2], dec[obj2])
            if not distances:
                if verbose:
                    log.debug("No matching visits found for objs: %d and %d" %
                              (obj1, obj2))
                continue

            finiteEntries, = np.where(np.isfinite(distances))
            # Need at least 2 distances to get a finite sample stdev
            if len(finiteEntries) > 1:
                # ddof=1 to get sample standard deviation (e.g., 1/(n-1))
                rmsDist = np.std(np.array(distances)[finiteEntries], ddof=1)
                rmsDistances.append(rmsDist)

    # return quantity
    rmsDistances = np.array(rmsDistances) * u.radian
    return rmsDistances


def calcSepOutliers(groupView, annulus, magRange, verbose=False):
    """Calculate the RMS distance of a set of matched objects over visits.
    Parameters
    ----------
    groupView : lsst.afw.table.GroupView
        GroupView object of matched observations from MultiMatch.
    annulus : length-2 `astropy.units.Quantity`
        Distance range (i.e., arcmin) in which to compare objects.
        E.g., `annulus=np.array([19, 21]) * u.arcmin` would consider all
        objects separated from each other between 19 and 21 arcminutes.
    magRange : length-2 `astropy.units.Quantity`
        Magnitude range from which to select objects.
    verbose : bool, optional
        Output additional information on the analysis steps.
    Returns
    -------
    rmsDistances : `astropy.units.Quantity`
        RMS angular separations of a set of matched objects over visits.
    """

    log = logging.getLogger(__name__)

    # First we make a list of the keys that we want the fields for
    importantKeys = [groupView.schema.find(name).key for
                     name in ['id', 'coord_ra', 'coord_dec',
                              'object', 'visit', 'base_PsfFlux_mag']]

    minMag, maxMag = magRange.to(u.mag).value

    def magInRange(cat):
        mag = cat.get('base_PsfFlux_mag')
        w, = np.where(np.isfinite(mag))
        medianMag = np.median(mag[w])
        return minMag <= medianMag and medianMag < maxMag

    groupViewInMagRange = groupView.where(magInRange)

    # List of lists of id, importantValue
    matchKeyOutput = [obj.get(key)
                      for key in importantKeys
                      for obj in groupViewInMagRange.groups]

    jump = len(groupViewInMagRange)

    ra = matchKeyOutput[1*jump:2*jump]
    dec = matchKeyOutput[2*jump:3*jump]
    visit = matchKeyOutput[4*jump:5*jump]

    # Calculate the mean position of each object from its constituent visits
    # `aggregate` calulates a quantity for each object in the groupView.
    meanRa = groupViewInMagRange.aggregate(averageRaFromCat)
    meanDec = groupViewInMagRange.aggregate(averageDecFromCat)

    annulusRadians = arcminToRadians(annulus.to(u.arcmin).value)

    sepResiduals = list()
    for obj1, (ra1, dec1, visit1) in enumerate(zip(meanRa, meanDec, visit)):
        dist = sphDist(ra1, dec1, meanRa[obj1+1:], meanDec[obj1+1:])
        objectsInAnnulus, = np.where((annulusRadians[0] <= dist)
                                     & (dist < annulusRadians[1]))
        objectsInAnnulus += obj1 + 1
        for obj2 in objectsInAnnulus:
            distances = matchVisitComputeDistance(
                visit[obj1], ra[obj1], dec[obj1],
                visit[obj2], ra[obj2], dec[obj2])
            if not distances:
                if verbose:
                    log.debug("No matching visits found for objs: %d and %d" %
                              (obj1, obj2))
                continue

            finiteEntries, = np.where(np.isfinite(distances))
            # Need at least 3 matched pairs so that the median position makes sense
            if len(finiteEntries) >= 3:
                okdist = np.array(distances)[finiteEntries]
                # Get rid of zeros from stars measured against themselves:
                realdist, = np.where(okdist > 0.0)
                if np.size(realdist) > 0:
                    sepResiduals.append(np.abs(okdist[realdist] - np.median(okdist[realdist])))

    # return quantity
    # import pdb; pdb.set_trace()
    if len(sepResiduals) > 0:
        sepResiduals = np.concatenate(np.array(sepResiduals)) * u.radian
    return sepResiduals


def matchVisitComputeDistance(visit_obj1, ra_obj1, dec_obj1,
                              visit_obj2, ra_obj2, dec_obj2):
    """Calculate obj1-obj2 distance for each visit in which both objects are seen.
    For each visit shared between visit_obj1 and visit_obj2,
    calculate the spherical distance between the obj1 and obj2.
    visit_obj1 and visit_obj2 are assumed to be unsorted.
    Parameters
    ----------
    visit_obj1 : scalar, list, or numpy.array of int or str
        List of visits for object 1.
    ra_obj1 : scalar, list, or numpy.array of float
        List of RA in each visit for object 1.  [radians]
    dec_obj1 : scalar, list or numpy.array of float
        List of Dec in each visit for object 1. [radians]
    visit_obj2 : list or numpy.array of int or str
        List of visits for object 2.
    ra_obj2 : list or numpy.array of float
        List of RA in each visit for object 2.  [radians]
    dec_obj2 : list or numpy.array of float
        List of Dec in each visit for object 2.  [radians]
    Results
    -------
    list of float
        spherical distances (in radians) for matching visits.
    """
    distances = []
    visit_obj1_idx = np.argsort(visit_obj1)
    visit_obj2_idx = np.argsort(visit_obj2)
    j_raw = 0
    j = visit_obj2_idx[j_raw]
    for i in visit_obj1_idx:
        while (visit_obj2[j] < visit_obj1[i]) and (j_raw < len(visit_obj2_idx)-1):
            j_raw += 1
            j = visit_obj2_idx[j_raw]
        if visit_obj2[j] == visit_obj1[i]:
            if np.isfinite([ra_obj1[i], dec_obj1[i],
                            ra_obj2[j], dec_obj2[j]]).all():
                distances.append(sphDist(ra_obj1[i], dec_obj1[i],
                                         ra_obj2[j], dec_obj2[j]))
    return distances


def calcRmsDistancesVsRef(groupView, refVisit, magRange, band, verbose=False):
    """Calculate the RMS distance of a set of matched objects over visits.
    Parameters
    ----------
    groupView : lsst.afw.table.GroupView
        GroupView object of matched observations from MultiMatch.
    magRange : length-2 `astropy.units.Quantity`
        Magnitude range from which to select objects.
    verbose : bool, optional
        Output additional information on the analysis steps.
    Returns
    -------
    rmsDistances : `astropy.units.Quantity`
        RMS angular separations of a set of matched objects over visits.
    separations : `astropy.units.Quantity`
        Angular separations of the set a matched objects.
    """

    minMag, maxMag = magRange.to(u.mag).value

    def magInRange(cat):
        mag = cat.get('base_PsfFlux_mag')
        w, = np.where(np.isfinite(mag))
        medianMag = np.median(mag[w])
        return minMag <= medianMag and medianMag < maxMag

    groupViewInMagRange = groupView.where(magInRange)

    # Get lists of the unique objects and visits:
    uniqObj = groupViewInMagRange.ids
    uniqVisits = set()
    for id in uniqObj:
        for v, f in zip(groupViewInMagRange[id].get('visit'),
                        groupViewInMagRange[id].get('filt')):
            if f == band:
                uniqVisits.add(v)

    uniqVisits = list(uniqVisits)

    if not isinstance(refVisit, int):
        refVisit = int(refVisit)

    if refVisit in uniqVisits:
        # Remove the reference visit from the set of visits:
        uniqVisits.remove(refVisit)

    rmsDistances = list()

    # Loop over visits, calculating the RMS for each:
    for vis in uniqVisits:

        distancesVisit = list()

        for obj in uniqObj:
            visMatch = np.where(groupViewInMagRange[obj].get('visit') == vis)
            refMatch = np.where(groupViewInMagRange[obj].get('visit') == refVisit)

            raObj = groupViewInMagRange[obj].get('coord_ra')
            decObj = groupViewInMagRange[obj].get('coord_dec')

            # Require it to have a match in both the reference and visit image:
            if np.size(visMatch[0]) > 0 and np.size(refMatch[0]) > 0:
                distances = sphDist(raObj[refMatch], decObj[refMatch],
                                    raObj[visMatch], decObj[visMatch])

                distancesVisit.append(distances)

        finiteEntries = np.where(np.isfinite(distancesVisit))[0]
        # Need at least 2 distances to get a finite sample stdev
        if len(finiteEntries) > 1:
            # Calculate the RMS of these offsets:
            # ddof=1 to get sample standard deviation (e.g., 1/(n-1))
            pos_rms_rad = np.std(np.array(distancesVisit)[finiteEntries], ddof=1)
            pos_rms_mas = geom.radToMas(pos_rms_rad)  # milliarcsec
            rmsDistances.append(pos_rms_mas)

        else:
            rmsDistances.append(np.nan)

    rmsDistances = np.array(rmsDistances) * u.marcsec
    return rmsDistances


def radiansToMilliarcsec(rad):
    return np.rad2deg(rad)*3600*1000


def arcminToRadians(arcmin):
    return np.deg2rad(arcmin/60)
