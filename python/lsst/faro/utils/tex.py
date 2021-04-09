import operator

import astropy.units as u
import numpy as np
import treecorr

from lsst.faro.utils.matcher import merge_catalogs


__all__ = ("TraceSize", "PsfTraceSizeDiff", "E1", "E2", "E1Resids", "E2Resids", 
           "RhoStatistics", "corrSpin0", "corrSpin2", "calculate_tex")


class TraceSize(object):
    """Functor to calculate trace radius size for sources.
    """
    def __init__(self, column):
        self.column = column

    def __call__(self, catalog):
        srcSize = np.sqrt(0.5*(catalog[self.column + "_xx"] + catalog[self.column + "_yy"]))
        return np.array(srcSize)


class PsfTraceSizeDiff(object):
    """Functor to calculate trace radius size difference (%) between object and
    PSF model.
    """
    def __init__(self, column, psfColumn):
        self.column = column
        self.psfColumn = psfColumn

    def __call__(self, catalog):
        srcSize = np.sqrt(0.5*(catalog[self.column + "_xx"] + catalog[self.column + "_yy"]))
        psfSize = np.sqrt(0.5*(catalog[self.psfColumn + "_xx"] + catalog[self.psfColumn + "_yy"]))
        sizeDiff = 100*(srcSize - psfSize)/(0.5*(srcSize + psfSize))
        return np.array(sizeDiff)
    

class E1(object):
    """Function to calculate e1 ellipticities from a given catalog.
    Parameters
    ----------
    column : `str`
        The name of the shape measurement algorithm. It should be one of
        ("base_SdssShape", "ext_shapeHSM_HsmSourceMoments") or
        ("base_SdssShape_psf", "ext_shapeHSM_HsmPsfMoments") for corresponding
        PSF ellipticities.
    unitScale : `float`, optional
        A numerical scaling factor to multiply the ellipticity.
    Returns
    -------
    e1 : `numpy.array`
        A numpy array of e1 ellipticity values.
    """
    def __init__(self, column, unitScale=1.0):
        self.column = column
        self.unitScale = unitScale

    def __call__(self, catalog):
        e1 = ((catalog[self.column + "_xx"]
               - catalog[self.column + "_yy"])/(catalog[self.column + "_xx"]
                                                + catalog[self.column + "_yy"]))
        return np.array(e1)*self.unitScale


class E2(object):
    """Function to calculate e2 ellipticities from a given catalog.
    Parameters
    ----------
    column : `str`
        The name of the shape measurement algorithm. It should be one of
        ("base_SdssShape", "ext_shapeHSM_HsmSourceMoments") or
        ("base_SdssShape_psf", "ext_shapeHSM_HsmPsfMoments") for corresponding
        PSF ellipticities.
    unitScale : `float`, optional
        A numerical scaling factor to multiply the ellipticity.
    Returns
    -------
    e2 : `numpy.array`
        A numpy array of e2 ellipticity values.
    """
    def __init__(self, column, unitScale=1.0):
        self.column = column
        self.unitScale = unitScale

    def __call__(self, catalog):
        e2 = (2.0*catalog[self.column + "_xy"]/(catalog[self.column + "_xx"] + catalog[self.column + "_yy"]))
        return np.array(e2)*self.unitScale


class E1Resids(object):
    """Functor to calculate e1 ellipticity residuals from an object catalog
    and PSF model.
    Parameters
    ----------
    column : `str`
        The name of the shape measurement algorithm. It should be one of
        ("base_SdssShape", "ext_shapeHSM_HsmSourceMoments").
    psfColumn : `str`
        The name used for PSF shape measurements from the same algorithm.
        It must be one of ("base_SdssShape_psf", "ext_shapeHSM_HsmPsfMoments")
        and correspond to the algorithm name specified for ``column``.
    unitScale : `float`, optional
        A numerical scaling factor to multiply both the object and PSF
        ellipticities.
    Returns
    -------
    e1Resids : `numpy.array`
        A numpy array of e1 residual ellipticity values.
    """
    def __init__(self, column, psfColumn, unitScale=1.0):
        self.column = column
        self.psfColumn = psfColumn
        self.unitScale = unitScale

    def __call__(self, catalog):
        srcE1func = E1(self.column, self.unitScale)
        psfE1func = E1(self.psfColumn, self.unitScale)

        srcE1 = srcE1func(catalog)
        psfE1 = psfE1func(catalog)

        e1Resids = srcE1 - psfE1
        return e1Resids


class E2Resids(object):
    """Functor to calculate e2 ellipticity residuals from an object catalog
    and PSF model.
    Parameters
    ----------
    column : `str`
        The name of the shape measurement algorithm. It should be one of
        ("base_SdssShape", "ext_shapeHSM_HsmSourceMoments").
    psfColumn : `str`
        The name used for PSF shape measurements from the same algorithm.
        It must be one of ("base_SdssShape_psf", "ext_shapeHSM_HsmPsfMoments")
        and correspond to the algorithm name specified for ``column``.
    unitScale : `float`, optional
        A numerical scaling factor to multiply both the object and PSF
        ellipticities.
    Returns
    -------
    e2Resids : `numpy.array`
        A numpy array of e2 residual ellipticity values.
    """
    def __init__(self, column, psfColumn, unitScale=1.0):
        self.column = column
        self.psfColumn = psfColumn
        self.unitScale = unitScale

    def __call__(self, catalog):
        srcE2func = E2(self.column, self.unitScale)
        psfE2func = E2(self.psfColumn, self.unitScale)

        srcE2 = srcE2func(catalog)
        psfE2 = psfE2func(catalog)

        e2Resids = srcE2 - psfE2
        return e2Resids

    
class RhoStatistics(object):
    """Functor to compute Rho statistics given star catalog and PSF model.
    For detailed description of Rho statistics, refer to
    Rowe (2010) and Jarvis et al., (2016).
    Parameters
    ----------
    column : `str`
        The name of the shape measurement algorithm. It should be one of
        ("base_SdssShape", "ext_shapeHSM_HsmSourceMoments").
    psfColumn : `str`
        The name used for PSF shape measurements from the same algorithm.
        It must be one of ("base_SdssShape_psf", "ext_shapeHSM_HsmPsfMoments")
        and correspond to the algorithm name specified for ``column``.
    **kwargs
        Additional keyword arguments passed to treecorr. See
        https://rmjarvis.github.io/TreeCorr/_build/html/gg.html for details.
    Returns
    -------
    rhoStats : `dict` [`int`, `treecorr.KKCorrelation` or
                              `treecorr.GGCorrelation`]
        A dictionary with keys 0..5, containing one `treecorr.KKCorrelation`
        object (key 0) and five `treecorr.GGCorrelation` objects corresponding
        to Rho statistic indices. rho0 corresponds to autocorrelation function
        of PSF size residuals.
    """
    def __init__(self, column, psfColumn, **kwargs):
        self.column = column
        self.psfColumn = psfColumn
        self.e1Func = E1(self.psfColumn)
        self.e2Func = E2(self.psfColumn)
        self.e1ResidsFunc = E1Resids(self.column, self.psfColumn)
        self.e2ResidsFunc = E2Resids(self.column, self.psfColumn)
        self.traceSizeFunc = TraceSize(self.column)
        self.psfTraceSizeFunc = TraceSize(self.psfColumn)
        self.kwargs = kwargs

    def __call__(self, catalog):
        e1 = self.e1Func(catalog)
        e2 = self.e2Func(catalog)
        e1Res = self.e1ResidsFunc(catalog)
        e2Res = self.e2ResidsFunc(catalog)
        traceSize2 = self.traceSizeFunc(catalog)**2
        psfTraceSize2 = self.psfTraceSizeFunc(catalog)**2
        SizeRes = (traceSize2 - psfTraceSize2)/(0.5*(traceSize2 + psfTraceSize2))

        isFinite = np.isfinite(e1Res) & np.isfinite(e2Res) & np.isfinite(SizeRes)
        e1 = e1[isFinite]
        e2 = e2[isFinite]
        e1Res = e1Res[isFinite]
        e2Res = e2Res[isFinite]
        SizeRes = SizeRes[isFinite]

        # Scale the SizeRes by ellipticities
        e1SizeRes = e1*SizeRes
        e2SizeRes = e2*SizeRes

        # Package the arguments to capture auto-/cross-correlations for the
        # Rho statistics.
        args = {0: (SizeRes, None),
                1: (e1Res, e2Res, None, None),
                2: (e1, e2, e1Res, e2Res),
                3: (e1SizeRes, e2SizeRes, None, None),
                4: (e1Res, e2Res, e1SizeRes, e2SizeRes),
                5: (e1, e2, e1SizeRes, e2SizeRes)}

        ra = np.rad2deg(catalog["coord_ra"][isFinite])*60.  # arcmin
        dec = np.rad2deg(catalog["coord_dec"][isFinite])*60.  # arcmin

        # Pass the appropriate arguments to the correlator and build a dict
        rhoStats = {rhoIndex: corrSpin2(ra, dec, *(args[rhoIndex]), raUnits="arcmin", decUnits="arcmin",
                                        **self.kwargs) for rhoIndex in range(1, 6)}
        rhoStats[0] = corrSpin0(ra, dec, *(args[0]), raUnits="arcmin", decUnits="arcmin", **self.kwargs)

        return rhoStats
    

def corrSpin0(ra, dec, k1, k2=None, raUnits="degrees", decUnits="degrees", **treecorrKwargs):
    """Function to compute correlations between at most two scalar fields.
    This is used to compute Rho0 statistics, given the appropriate spin-0
    (scalar) fields, usually fractional size residuals.
    Parameters
    ----------
    ra : `numpy.array`
        The right ascension values of entries in the catalog.
    dec : `numpy.array`
        The declination values of entries in the catalog.
    k1 : `numpy.array`
        The primary scalar field.
    k2 : `numpy.array`, optional
        The secondary scalar field.
        Autocorrelation of the primary field is computed if `None` (default).
    raUnits : `str`, optional
        Unit of the right ascension values.
        Valid options are "degrees", "arcmin", "arcsec", "hours" or "radians".
    decUnits : `str`, optional
        Unit of the declination values.
        Valid options are "degrees", "arcmin", "arcsec", "hours" or "radians".
    **treecorrKwargs
        Keyword arguments to be passed to `treecorr.KKCorrelation`.
    Returns
    -------
    xy : `treecorr.KKCorrelation`
        A `treecorr.KKCorrelation` object containing the correlation function.
    """

    xy = treecorr.KKCorrelation(**treecorrKwargs)
    catA = treecorr.Catalog(ra=ra, dec=dec, k=k1, ra_units=raUnits,
                            dec_units=decUnits)
    if k2 is None:
        # Calculate the auto-correlation
        xy.process(catA)
    else:
        catB = treecorr.Catalog(ra=ra, dec=dec, k=k2, ra_units=raUnits,
                                dec_units=decUnits)
        # Calculate the cross-correlation
        xy.process(catA, catB)

    return xy


def corrSpin2(ra, dec, g1a, g2a, g1b=None, g2b=None, raUnits="degrees", decUnits="degrees", **treecorrKwargs):
    """Function to compute correlations between at most two shear-like fields.
    This is used to compute Rho statistics, given the appropriate spin-2
    (shear-like) fields.
    Parameters
    ----------
    ra : `numpy.array`
        The right ascension values of entries in the catalog.
    dec : `numpy.array`
        The declination values of entries in the catalog.
    g1a : `numpy.array`
        The first component of the primary shear-like field.
    g2a : `numpy.array`
        The second component of the primary shear-like field.
    g1b : `numpy.array`, optional
        The first component of the secondary shear-like field.
        Autocorrelation of the primary field is computed if `None` (default).
    g2b : `numpy.array`, optional
        The second component of the secondary shear-like field.
        Autocorrelation of the primary field is computed if `None` (default).
    raUnits : `str`, optional
        Unit of the right ascension values.
        Valid options are "degrees", "arcmin", "arcsec", "hours" or "radians".
    decUnits : `str`, optional
        Unit of the declination values.
        Valid options are "degrees", "arcmin", "arcsec", "hours" or "radians".
    **treecorrKwargs
        Keyword arguments to be passed to `treecorr.GGCorrelation`.
    Returns
    -------
    xy : `treecorr.GGCorrelation`
        A `treecorr.GGCorrelation` object containing the correlation function.
    """
    xy = treecorr.GGCorrelation(**treecorrKwargs)
    catA = treecorr.Catalog(ra=ra, dec=dec, g1=g1a, g2=g2a, ra_units=raUnits,
                            dec_units=decUnits)
    if g1b is None or g2b is None:
        # Calculate the auto-correlation
        xy.process(catA)
    else:
        catB = treecorr.Catalog(ra=ra, dec=dec, g1=g1b, g2=g2b, ra_units=raUnits,
                                dec_units=decUnits)
        # Calculate the cross-correlation
        xy.process(catA, catB)

    return xy

def calculate_tex(catalogs, photo_calibs, astrom_calibs, config):
    """Compute ellipticity residual correlation metrics.
    """
    
    catalog = merge_catalogs(catalogs, photo_calibs, astrom_calibs)
        
    # Filtering should be pulled out into a separate function for standard quality selections
    snr_min = 50
    selection = (catalog['base_ClassificationExtendedness_value'] < 0.5) \
        & ((catalog['slot_PsfFlux_instFlux'] / catalog['slot_PsfFlux_instFluxErr']) > snr_min) \
        & (catalog['deblend_nChild'] == 0)
    
    n_min_sources = 50
    if np.sum(selection) < n_min_sources:
        return Struct(measurement=Measurement(metric_name, np.nan*u.Unit('')))
    
    treecorr_kwargs = dict(nbins=config.nbins, 
                           min_sep=config.min_sep, 
                           max_sep=config.max_sep, 
                           sep_units='arcmin')
    rho_statistics = RhoStatistics(config.column, config.column_psf, **treecorr_kwargs)
    xy = rho_statistics(catalog[selection])[config.rho_stat]
    
    radius = np.exp(xy.meanlogr) * u.arcmin
    if config.rho_stat == 0:
        corr = xy.xi * u.Unit('')
        corr_err = np.sqrt(xy.varxip) * u.Unit('')
    else:
        corr = xy.xip * u.Unit('')
        corr_err = np.sqrt(xy.varxip) * u.Unit('')
        
    result = dict(radius=radius, corr=corr, corr_err=corr_err)
    return result
