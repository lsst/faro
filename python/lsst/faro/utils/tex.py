import operator

import astropy.units as u
import numpy as np
import treecorr

from lsst.faro.utils.coord_util import averageRaFromCat, averageDecFromCat


def correlation_function_ellipticity_from_matches(matches, **kwargs):
    """Compute shear-shear correlation function for ellipticity residual from a 'MatchedMultiVisitDataset' object.
    Convenience function for calling correlation_function_ellipticity.
    Parameters
    ----------
    matches : `lsst.verify.Blob`
        - The matched catalogs to analyze.
    Returns
    -------
    r, xip, xip_err : each a np.array(dtype=float)
        - The bin centers, two-point correlation, and uncertainty.
    """
    ra = matches.aggregate(averageRaFromCat) * u.radian
    dec = matches.aggregate(averageDecFromCat) * u.radian

    e1_res = matches.aggregate(medianEllipticity1ResidualsFromCat)
    e2_res = matches.aggregate(medianEllipticity2ResidualsFromCat)

    return correlation_function_ellipticity(ra, dec, e1_res, e2_res, **kwargs)


def correlation_function_ellipticity(ra, dec, e1_res, e2_res,
                                     nbins=20, min_sep=0.25, max_sep=20,
                                     sep_units='arcmin', verbose=False):
    """Compute shear-shear correlation function from ra, dec, g1, g2.
    Default parameters for nbins, min_sep, max_sep chosen to cover
       an appropriate range to calculate TE1 (<=1 arcmin) and TE2 (>=5 arcmin).
    Parameters
    ----------
    ra : numpy.array
        Right ascension of points [radians]
    dec : numpy.array
        Declination of points [radians]
    e1_res : numpy.array
        Residual ellipticity 1st component
    e2_res : numpy.array
        Residual ellipticity 2nd component
    nbins : float, optional
        Number of bins over which to analyze the two-point correlation
    min_sep : float, optional
        Minimum separation over which to analyze the two-point correlation
    max_sep : float, optional
        Maximum separation over which to analyze the two-point correlation
    sep_units : str, optional
        Specify the units of min_sep and max_sep
    verbose : bool
        Request verbose output from `treecorr`.
        verbose=True will use verbose=2 for `treecorr.GGCorrelation`.
    Returns
    -------
    r, xip, xip_err : each a np.array(dtype=float)
        - The bin centers, two-point correlation, and uncertainty.
    """
    # Translate to 'verbose_level' here to refer to the integer levels in TreeCorr
    # While 'verbose' is more generically what is being passed around
    #   for verbosity within 'validate_drp'
    if verbose:
        verbose_level = 2
    else:
        verbose_level = 0

    catTree = treecorr.Catalog(ra=ra, dec=dec, g1=e1_res, g2=e2_res,
                               dec_units='radian', ra_units='radian')
    gg = treecorr.GGCorrelation(nbins=nbins, min_sep=min_sep, max_sep=max_sep,
                                sep_units=sep_units,
                                verbose=verbose_level)
    gg.process(catTree)
    r = np.exp(gg.meanlogr) * u.arcmin
    xip = gg.xip * u.Unit('')
    # FIXME: Remove treecorr < 4 support
    try:
        # treecorr > 4
        xip_err = np.sqrt(gg.varxip) * u.Unit('')
    except AttributeError:
        # treecorr < 4
        xip_err = np.sqrt(gg.varxi) * u.Unit('')

    return (r, xip, xip_err)


def select_bin_from_corr(r, xip, xip_err, radius=1*u.arcmin, operator=operator.le):
    """Aggregate measurements for r less than (or greater than) radius.
    Returns aggregate measurement for all entries where operator(r, radius).
    E.g.,
     * Passing radius=5, operator=operator.le will return averages for r<=5
     * Passing radius=2, operator=operator.gt will return averages for r >2
    Written with the use of correlation functions in mind, thus the naming
    but generically just returns averages of the arrays xip and xip_err
    where the condition is satsified
    Parameters
    ----------
    r : numpy.array
        radius
    xip : numpy.array
        correlation
    xip_err : numpy.array
        correlation uncertainty
    operator : Operation in the 'operator' module: le, ge, lt, gt
    Returns
    -------
    avg_xip, avg_xip_err : (float, float)
    """
    w, = np.where(operator(r, radius))

    avg_xip = np.average(xip[w])
    avg_xip_err = np.average(xip_err[w])

    return avg_xip, avg_xip_err


def medianEllipticity1ResidualsFromCat(cat):
    """Compute the median real ellipticty residuals from a catalog of measurements.
    Parameters
    ----------
    cat : collection
         Object with .get method for 'e1', 'psf_e1' that returns radians.
    Returns
    -------
    e1_median : `float`
        Median imaginary ellipticity residual.
    """
    e1_median = np.median(cat.get('e1') - cat.get('psf_e1'))
    return e1_median


def medianEllipticity2ResidualsFromCat(cat):
    """Compute the median imaginary ellipticty residuals from a catalog of measurements.
    Parameters
    ----------
    cat : collection
         Object with .get method for 'e2', 'psf_e2' that returns radians.
    Returns
    -------
    e2_median : `float`
        Median imaginary ellipticity residual.
    """
    e2_median = np.median(cat.get('e2') - cat.get('psf_e2'))
    return e2_median
