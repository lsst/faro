# This file is part of faro.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import astropy.units as u
import numpy as np
import treecorr


__all__ = (
    "TraceSize",
    "PsfTraceSizeDiff",
    "E1",
    "E2",
    "E1Resids",
    "E2Resids",
    "RhoStatistics",
    "corrSpin0",
    "corrSpin2",
    "calculateTEx",
)


class TraceSize(object):
    """Functor to calculate trace radius size for sources. ixxColumn and
    iyyColumn are strings and must be defined in the Task Config.
    """

    def __init__(self, ixxColumn, iyyColumn):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn

    def __call__(self, catalog):
        srcSize = np.sqrt(0.5 * (catalog[self.ixxColumn] + catalog[self.iyyColumn]))
        return np.array(srcSize)


class PsfTraceSizeDiff(object):
    """Functor to calculate trace radius size difference (%) between object and
    PSF model. Inherits from functor TraceSize.
    """

    def __init__(self, ixxColumn, iyyColumn, ixxPsfColumn, iyyPsfColumn):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixxPsfColumn = ixxPsfColumn
        self.iyyPsfColumn = iyyPsfColumn

        self.traceSizeFunc = TraceSize(self.ixxColumn, self.iyyColumn)
        self.psfTraceSizeFunc = TraceSize(self.ixxPsfColumn, self.iyyPsfColumn)

    def __call__(self, catalog):
        srcSize = self.traceSizeFunc(catalog)
        psfSize = self.psfTraceSizeFunc(catalog)
        sizeDiff = 100 * (srcSize - psfSize) / (0.5 * (srcSize + psfSize))
        return np.array(sizeDiff)


class E1(object):
    """Function to calculate e1 ellipticities from a given catalog.
    Parameters
    ----------
    ixxColumn : `str`
        The name of the for corresponding ixx ellipticities column.
    iyyColumn : `str`
        The name of the for corresponding iyy ellipticities column.
    ixyColumn : `str`
        The name of the for corresponding ixy ellipticities column.
    unitScale : `float`, optional
        A numerical scaling factor to multiply the ellipticity.
    shearConvention: `bool`, optional
        Option to use shear convention. When set to False, the distortion
        convention is used.
    Returns
    -------
    e1 : `numpy.array`
        A numpy array of e1 ellipticity values.
    """

    def __init__(
        self, ixxColumn, iyyColumn, ixyColumn, unitScale=1.0, shearConvention=False
    ):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixyColumn = ixyColumn
        self.unitScale = unitScale
        self.shearConvention = shearConvention

    def __call__(self, catalog):
        xx = catalog[self.ixxColumn]
        yy = catalog[self.iyyColumn]
        if self.shearConvention:
            xy = catalog[self.ixyColumn]
            e1 = (xx - yy) / (xx + yy + 2.0 * np.sqrt(xx * yy - xy ** 2))
        else:
            e1 = (xx - yy) / (xx + yy)
        return np.array(e1) * self.unitScale


class E2(object):
    """Function to calculate e2 ellipticities from a given catalog.
    Parameters
    ----------
    ixxColumn : `str`
        The name of the for corresponding ixx ellipticities column.
    iyyColumn : `str`
        The name of the for corresponding iyy ellipticities column.
    ixyColumn : `str`
        The name of the for corresponding ixy ellipticities column.
    unitScale : `float`, optional
        A numerical scaling factor to multiply the ellipticity.
    shearConvention: `bool`, optional
        Option to use shear convention. When set to False, the distortion
        convention is used.
    Returns
    -------
    e2 : `numpy.array`
        A numpy array of e2 ellipticity values.
    """

    def __init__(
        self, ixxColumn, iyyColumn, ixyColumn, unitScale=1.0, shearConvention=False
    ):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixyColumn = ixyColumn
        self.unitScale = unitScale
        self.shearConvention = shearConvention

    def __call__(self, catalog):
        xx = catalog[self.ixxColumn]
        yy = catalog[self.iyyColumn]
        xy = catalog[self.ixyColumn]
        if self.shearConvention:
            e2 = (2.0 * xy) / (xx + yy + 2.0 * np.sqrt(xx * yy - xy ** 2))
        else:
            e2 = (2.0 * xy) / (xx + yy)
        return np.array(e2) * self.unitScale


class E1Resids(object):
    """Functor to calculate e1 ellipticity residuals from an object catalog
    and PSF model.
    Parameters
    ----------
    ixxColumn : `str`
        The name of the for corresponding ixx ellipticities column.
    iyyColumn : `str`
        The name of the for corresponding iyy ellipticities column.
    ixyColumn : `str`
        The name of the for corresponding ixy ellipticities column.
    ixxPsfColumn : `str`
        The name of the for corresponding ixx psf ellipticities column.
    iyyPsfColumn : `str`
        The name of the for corresponding iyy psf ellipticities column.
    ixyPsfColumn : `str`
        The name of the for corresponding ixy psf ellipticities column.
    unitScale : `float`, optional
        A numerical scaling factor to multiply both the object and PSF
        ellipticities.
    shearConvention: `bool`, optional
        Option to use shear convention. When set to False, the distortion
        convention is used.
    Returns
    -------
    e1Resids : `numpy.array`
        A numpy array of e1 residual ellipticity values.
    """

    def __init__(
        self,
        ixxColumn,
        iyyColumn,
        ixxPsfColumn,
        iyyPsfColumn,
        ixyColumn,
        ixyPsfColumn,
        unitScale=1.0,
        shearConvention=False,
    ):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixyColumn = ixyColumn
        self.ixxPsfColumn = ixxPsfColumn
        self.iyyPsfColumn = iyyPsfColumn
        self.ixyPsfColumn = ixyPsfColumn

        self.unitScale = unitScale
        self.shearConvention = shearConvention

    def __call__(self, catalog):
        srcE1func = E1(
            self.ixxColumn,
            self.iyyColumn,
            self.ixyColumn,
            self.unitScale,
            self.shearConvention,
        )
        psfE1func = E1(
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyPsfColumn,
            self.unitScale,
            self.shearConvention,
        )

        srcE1 = srcE1func(catalog)
        psfE1 = psfE1func(catalog)

        e1Resids = srcE1 - psfE1
        return e1Resids


class E2Resids(object):
    """Functor to calculate e2 ellipticity residuals from an object catalog
    and PSF model.
    Parameters
    ----------
    ixxColumn : `str`
        The name of the for corresponding ixx ellipticities column.
    iyyColumn : `str`
        The name of the for corresponding iyy ellipticities column.
    ixyColumn : `str`
        The name of the for corresponding ixy ellipticities column.
    ixxPsfColumn : `str`
        The name of the for corresponding ixx psf ellipticities column.
    iyyPsfColumn : `str`
        The name of the for corresponding iyy psf ellipticities column.
    ixyPsfColumn : `str`
        The name of the for corresponding ixy psf ellipticities column.
    unitScale : `float`, optional
        A numerical scaling factor to multiply both the object and PSF
        ellipticities.
    shearConvention: `bool`, optional
        Option to use shear convention. When set to False, the distortion
        convention is used.
    Returns
    -------
    e2Resids : `numpy.array`
        A numpy array of e2 residual ellipticity values.
    """

    def __init__(
        self,
        ixxColumn,
        iyyColumn,
        ixxPsfColumn,
        iyyPsfColumn,
        ixyColumn,
        ixyPsfColumn,
        unitScale=1.0,
        shearConvention=False,
    ):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixyColumn = ixyColumn
        self.ixxPsfColumn = ixxPsfColumn
        self.iyyPsfColumn = iyyPsfColumn
        self.ixyPsfColumn = ixyPsfColumn
        self.unitScale = unitScale
        self.shearConvention = shearConvention

    def __call__(self, catalog):
        srcE2func = E2(
            self.ixxColumn,
            self.iyyColumn,
            self.ixyColumn,
            self.unitScale,
            self.shearConvention,
        )
        psfE2func = E2(
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyPsfColumn,
            self.unitScale,
            self.shearConvention,
        )

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
    shearConvention: `bool`, optional
        Option to use shear convention. When set to False, the distortion
        convention is used.
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

    def __init__(
        self,
        ixxColumn,
        iyyColumn,
        ixxPsfColumn,
        iyyPsfColumn,
        raColumn,
        decColumn,
        ixyColumn,
        ixyPsfColumn,
        shearConvention=False,
        **kwargs
    ):
        self.ixxColumn = ixxColumn
        self.iyyColumn = iyyColumn
        self.ixyColumn = ixyColumn
        self.ixxPsfColumn = ixxPsfColumn
        self.iyyPsfColumn = iyyPsfColumn
        self.ixyPsfColumn = ixyPsfColumn
        self.shearConvention = shearConvention
        self.raColumn = raColumn
        self.decColumn = decColumn
        self.e1Func = E1(
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyPsfColumn,
            shearConvention=self.shearConvention,
        )
        self.e2Func = E2(
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyPsfColumn,
            shearConvention=self.shearConvention,
        )
        self.e1ResidsFunc = E1Resids(
            self.ixxColumn,
            self.iyyColumn,
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyColumn,
            self.ixyPsfColumn,
            shearConvention=self.shearConvention,
        )
        self.e2ResidsFunc = E2Resids(
            self.ixxColumn,
            self.iyyColumn,
            self.ixxPsfColumn,
            self.iyyPsfColumn,
            self.ixyColumn,
            self.ixyPsfColumn,
            shearConvention=self.shearConvention,
        )
        self.traceSizeFunc = TraceSize(self.ixxColumn, self.iyyColumn)
        self.psfTraceSizeFunc = TraceSize(self.ixxPsfColumn, self.iyyPsfColumn)
        self.kwargs = kwargs

    def __call__(self, catalog):
        e1 = self.e1Func(catalog)
        e2 = self.e2Func(catalog)
        e1Res = self.e1ResidsFunc(catalog)
        e2Res = self.e2ResidsFunc(catalog)
        traceSize2 = self.traceSizeFunc(catalog) ** 2
        psfTraceSize2 = self.psfTraceSizeFunc(catalog) ** 2
        SizeRes = (traceSize2 - psfTraceSize2) / (0.5 * (traceSize2 + psfTraceSize2))

        isFinite = np.isfinite(e1Res) & np.isfinite(e2Res) & np.isfinite(SizeRes)
        e1 = e1[isFinite]
        e2 = e2[isFinite]
        e1Res = e1Res[isFinite]
        e2Res = e2Res[isFinite]
        SizeRes = SizeRes[isFinite]

        # Scale the SizeRes by ellipticities
        e1SizeRes = e1 * SizeRes
        e2SizeRes = e2 * SizeRes

        # Package the arguments to capture auto-/cross-correlations for the
        # Rho statistics.
        args = {
            0: (SizeRes, None),
            1: (e1Res, e2Res, None, None),
            2: (e1, e2, e1Res, e2Res),
            3: (e1SizeRes, e2SizeRes, None, None),
            4: (e1Res, e2Res, e1SizeRes, e2SizeRes),
            5: (e1, e2, e1SizeRes, e2SizeRes),
        }

        ra = catalog[self.raColumn][isFinite] * 60.0  # arcmin
        dec = catalog[self.decColumn][isFinite] * 60.0  # arcmin

        # Pass the appropriate arguments to the correlator and build a dict
        rhoStats = {
            rhoIndex: corrSpin2(
                ra,
                dec,
                *(args[rhoIndex]),
                raUnits="arcmin",
                decUnits="arcmin",
                **self.kwargs
            )
            for rhoIndex in range(1, 6)
        }
        rhoStats[0] = corrSpin0(
            ra, dec, *(args[0]), raUnits="arcmin", decUnits="arcmin", **self.kwargs
        )

        return rhoStats


def corrSpin0(
    ra, dec, k1, k2=None, raUnits="degrees", decUnits="degrees", **treecorrKwargs
):
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
    catA = treecorr.Catalog(ra=ra, dec=dec, k=k1, ra_units=raUnits, dec_units=decUnits)
    if k2 is None:
        # Calculate the auto-correlation
        xy.process(catA)
    else:
        catB = treecorr.Catalog(
            ra=ra, dec=dec, k=k2, ra_units=raUnits, dec_units=decUnits
        )
        # Calculate the cross-correlation
        xy.process(catA, catB)

    return xy


def corrSpin2(
    ra,
    dec,
    g1a,
    g2a,
    g1b=None,
    g2b=None,
    raUnits="degrees",
    decUnits="degrees",
    **treecorrKwargs
):
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
    catA = treecorr.Catalog(
        ra=ra, dec=dec, g1=g1a, g2=g2a, ra_units=raUnits, dec_units=decUnits
    )
    if g1b is None or g2b is None:
        # Calculate the auto-correlation
        xy.process(catA)
    else:
        catB = treecorr.Catalog(
            ra=ra, dec=dec, g1=g1b, g2=g2b, ra_units=raUnits, dec_units=decUnits
        )
        # Calculate the cross-correlation
        xy.process(catA, catB)

    return xy


def calculateTEx(catalog, config, prependString):
    """Compute ellipticity residual correlation metrics using parquet table as input.
    Parameters
    ----------
    catalog : `pandas datafram`
        The catalog on which TE values will be calculated.
    config : `pex config`
        Task configuration.
    prependString : `str`
        The string to prepend to the band-specific columns. Typically a single letter
        filter e.g. 'g'.
    Returns
    -------
    result : `dict`
        A dictionary with entries for radius, corr, and corrErr.
    """

    if prependString is not None:
        ixxColumn = prependString + "_" + config.ixxColumn
        iyyColumn = prependString + "_" + config.iyyColumn
        ixyColumn = prependString + "_" + config.ixyColumn
        ixxPsfColumn = prependString + "_" + config.ixxPsfColumn
        iyyPsfColumn = prependString + "_" + config.iyyPsfColumn
        ixyPsfColumn = prependString + "_" + config.ixyPsfColumn
        extendednessColumn = prependString + "_" + config.extendednessColumn
        psfFluxColumn = prependString + "_" + config.psfFluxColumn
        psfFluxErrColumn = prependString + "_" + config.psfFluxErrColumn
    else:
        ixxColumn = config.ixxColumn
        iyyColumn = config.iyyColumn
        ixyColumn = config.ixyColumn
        ixxPsfColumn = config.ixxPsfColumn
        iyyPsfColumn = config.iyyPsfColumn
        ixyPsfColumn = config.ixyPsfColumn
        extendednessColumn = config.extendednessColumn
        psfFluxColumn = config.psfFluxColumn
        psfFluxErrColumn = config.psfFluxErrColumn


    nMinSources = 50
    if np.sum(selection) < nMinSources:
        return {"nomeas": np.nan * u.Unit("")}

    treecorrKwargs = dict(
        nbins=config.nbins,
        min_sep=config.minSep,
        max_sep=config.maxSep,
        sep_units="arcmin",
    )

    rhoStatisticsFunc = RhoStatistics(
        ixxColumn,
        iyyColumn,
        ixxPsfColumn,
        iyyPsfColumn,
        config.raColumn,
        config.decColumn,
        ixyColumn,
        ixyPsfColumn,
        shearConvention=config.shearConvention,
        **treecorrKwargs
    )
    xy = rhoStatisticsFunc(catalog[selection])[config.rhoStat]

    radius = np.exp(xy.meanlogr) * u.arcmin
    if config.rhoStat == 0:
        corr = xy.xi * u.Unit("")
        corrErr = np.sqrt(xy.varxip) * u.Unit("")
    else:
        corr = xy.xip * u.Unit("")
        corrErr = np.sqrt(xy.varxip) * u.Unit("")

    result = dict(radius=radius, corr=corr, corrErr=corrErr)
    return result
