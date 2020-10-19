import numpy as np

import lsst.geom as geom

def averageRaFromCat(cat):
    """Compute the average right ascension from a catalog of measurements.
    This function is used as an aggregate function to extract just RA
    from lsst.validate.drp.matchreduce.build_matched_dataset
    The actual computation involves both RA and Dec.
    The intent is to use this for a set of measurements of the same source
    but that's neither enforced nor required.
    Parameters
    ----------
    cat : collection
         Object with .get method for 'coord_ra', 'coord_dec' that returns radians.
    Returns
    -------
    ra_mean : `float`
        Mean RA in radians.
    """
    meanRa, meanDec = averageRaDecFromCat(cat)
    return meanRa


def averageDecFromCat(cat):
    """Compute the average declination from a catalog of measurements.
    This function is used as an aggregate function to extract just declination
    from lsst.validate.drp.matchreduce.build_matched_dataset
    The actual computation involves both RA and Dec.
    The intent is to use this for a set of measurements of the same source
    but that's neither enforced nor required.
    Parameters
    ----------
    cat : collection
         Object with .get method for 'coord_ra', 'coord_dec' that returns radians.
    Returns
    -------
    dec_mean : `float`
        Mean Dec in radians.
    """
    meanRa, meanDec = averageRaDecFromCat(cat)
    return meanDec


def averageRaDecFromCat(cat):
    """Calculate the average right ascension and declination from a catalog.
    Convenience wrapper around averageRaDec
    Parameters
    ----------
    cat : collection
         Object with .get method for 'coord_ra', 'coord_dec' that returns radians.
    Returns
    -------
    ra_mean : `float`
        Mean RA in radians.
    dec_mean : `float`
        Mean Dec in radians.
    """
    return averageRaDec(cat.get('coord_ra'), cat.get('coord_dec'))


def averageRaDec(ra, dec):
    """Calculate average RA, Dec from input lists using spherical geometry.
    Parameters
    ----------
    ra : `list` [`float`]
        RA in [radians]
    dec : `list` [`float`]
        Dec in [radians]
    Returns
    -------
    float, float
       meanRa, meanDec -- Tuple of average RA, Dec [radians]
    """
    assert(len(ra) == len(dec))

    angleRa = [geom.Angle(r, geom.radians) for r in ra]
    angleDec = [geom.Angle(d, geom.radians) for d in dec]
    coords = [geom.SpherePoint(ar, ad, geom.radians) for (ar, ad) in zip(angleRa, angleDec)]

    meanRa, meanDec = geom.averageSpherePoint(coords)

    return meanRa.asRadians(), meanDec.asRadians()


def sphDist(ra_mean, dec_mean, ra, dec):
    """Calculate distance on the surface of a unit sphere.
    Parameters
    ----------
    ra_mean : `float`
        Mean RA in radians.
    dec_mean : `float`
        Mean Dec in radians.
    ra : `numpy.array` [`float`]
        Array of RA in radians.
    dec : `numpy.array` [`float`]
        Array of Dec in radians.
    Notes
    -----
    Uses the Haversine formula to preserve accuracy at small angles.
    Law of cosines approach doesn't work well for the typically very small
    differences that we're looking at here.
    """
    # Haversine
    dra = ra - ra_mean
    ddec = dec - dec_mean
    a = np.square(np.sin(ddec/2)) + \
        np.cos(dec_mean)*np.cos(dec)*np.square(np.sin(dra/2))
    dist = 2 * np.arcsin(np.sqrt(a))

    # This is what the law of cosines would look like
    #    dist = np.arccos(np.sin(dec1)*np.sin(dec2) + np.cos(dec1)*np.cos(dec2)*np.cos(ra1 - ra2))

    # This will also work, but must run separately for each element
    # whereas the numpy version will run on either scalars or arrays:
    #   sp1 = geom.SpherePoint(ra1, dec1, geom.radians)
    #   sp2 = geom.SpherePoint(ra2, dec2, geom.radians)
    #   return sp1.separation(sp2).asRadians()

    return dist
