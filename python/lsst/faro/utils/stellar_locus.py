import numpy as np
import scipy.stats as scipyStats
from lsst.pipe.base import Struct

__all__ = (
    "stellarLocusResid",
    "calcP1P2",
    "getCoeffs",
    "p1CoeffsFromP2x0y0",
    "p2p1CoeffsFromLinearFit",
    "calcQuartileClippedStats",
)


def stellarLocusResid(gmags, rmags, imags, **filterargs):

    gr = gmags - rmags
    ri = rmags - imags

    # Also trim large values of r-i, since those will skew the linear regression
    okfitcolors = (
        (gr < 1.1)
        & (gr > 0.3)
        & (np.abs(ri) < 1.0)
        & np.isfinite(gmags)
        & np.isfinite(rmags)
        & np.isfinite(imags)
    )
    # Eventually switch to using orthogonal regression instead of linear (as in pipe-analysis)?

    slope, intercept, r_value, p_value, std_err = scipyStats.linregress(
        gr[okfitcolors], ri[okfitcolors]
    )
    p2p1coeffs = p2p1CoeffsFromLinearFit(slope, intercept, 0.3, slope * 0.3 + intercept)
    p1coeffs = p2p1coeffs.p1Coeffs.copy()
    # hack to put the zeros in for u, z coeffs
    p1coeffs.insert(0, 0.0)
    p1coeffs.insert(4, 0.0)
    p2coeffs = list(p2p1coeffs.p2Coeffs.copy())
    p2coeffs.insert(0, 0.0)
    p2coeffs.insert(4, 0.0)
    umags = np.zeros(len(gmags))
    zmags = np.zeros(len(gmags))
    p1_fit = calcP1P2([umags, gmags, rmags, imags, zmags], p1coeffs)
    p2_fit = calcP1P2([umags, gmags, rmags, imags, zmags], p2coeffs)
    okp1_fit = (p1_fit < 0.6) & (p1_fit > -0.2)

    # Do a second iteration, removing large (>3 sigma) outliers in p2:
    clippedStats = calcQuartileClippedStats(p2_fit[okp1_fit], 3.0)
    keep = np.abs(p2_fit) < clippedStats.clipValue

    slope, intercept, r_value, p_value, std_err = scipyStats.linregress(
        gr[okfitcolors & keep], ri[okfitcolors & keep]
    )
    p2p1coeffs = p2p1CoeffsFromLinearFit(slope, intercept, 0.3, slope * 0.3 + intercept)
    p1coeffs = p2p1coeffs.p1Coeffs.copy()
    # hack to put the zeros in for u, z coeffs
    p1coeffs.insert(0, 0.0)
    p1coeffs.insert(4, 0.0)
    p2coeffs = list(p2p1coeffs.p2Coeffs.copy())
    p2coeffs.insert(0, 0.0)
    p2coeffs.insert(4, 0.0)
    p1_fit = calcP1P2([umags, gmags, rmags, imags, zmags], p1coeffs)
    p2_fit = calcP1P2([umags, gmags, rmags, imags, zmags], p2coeffs)
    okp1_fit = (p1_fit < 0.6) & (p1_fit > -0.2)

    return p1_fit[okp1_fit], p2_fit[okp1_fit], p1coeffs, p2coeffs


def calcP1P2(mags, coeffs):
    # P1 =A′u+B′g+C′r+D′i+E′z+F′
    # P2′=Au+Bg+Cr+Di+Ez+F
    p1p2 = (
        float(coeffs[0]) * mags[0]
        + float(coeffs[1]) * mags[1]
        + float(coeffs[2]) * mags[2]
        + float(coeffs[3]) * mags[3]
        + float(coeffs[4]) * mags[4]
        + float(coeffs[5])
    )
    return p1p2


def getCoeffs():
    # Coefficients from the Ivezic+2004 paper. Warning - if possible, the Coefficients
    # should be derived from a fit to the stellar locus rather than these "fallback" values.
    ivezicCoeffs = {
        "P1s": [0.91, -0.495, -0.415, 0.0, 0.0, -1.28],
        "P1w": [0.0, 0.928, -0.556, -0.372, 0.0, -0.425],
        "P2s": [-0.249, 0.794, -0.555, 0.0, 0.0, 0.234],
        "P2w": [0.0, -0.227, 0.792, -0.567, 0.0, 0.050],
    }
    ivezicCoeffsHSC = {
        "P1s": [0.91, -0.495, -0.415, 0.0, 0.0, -1.28],
        "P1w": [0.0, 0.888, -0.427, -0.461, 0.0, -0.478],
        "P2s": [-0.249, 0.794, -0.555, 0.0, 0.0, 0.234],
        "P2w": [0.0, -0.274, 0.803, -0.529, 0.0, 0.041],
    }
    return ivezicCoeffs, ivezicCoeffsHSC


# Everything below this is copied directly from pipe_analysis/utils.py.
# Should we move all those functions here once pipe_analysis is rewritten?


def p1CoeffsFromP2x0y0(p2Coeffs, x0, y0):
    """Compute Ivezic P1 coefficients using the P2 coeffs and origin (x0, y0)
    Reference: Ivezic et al. 2004 (2004AN....325..583I)
    theta = arctan(mP1), where mP1 is the slope of the equivalent straight
                         line (the P1 line) from the P2 coeffs in the (x, y)
                         coordinate system and x = c1 - c2, y = c2 - c3
    P1 = cos(theta)*c1 + ((sin(theta) - cos(theta))*c2 - sin(theta)*c3 + deltaP1
    P1 = 0 at x0, y0 ==> deltaP1 = -cos(theta)*x0 - sin(theta)*y0
    Parameters
    ----------
    p2Coeffs : `list` of `float`
       List of the four P2 coefficients from which, along with the origin point
       (``x0``, ``y0``), to compute/derive the associated P1 coefficients.
    x0, y0 : `float`
       Coordinates at which to set P1 = 0 (i.e. the P1/P2 axis origin).
    Returns
    -------
    p1Coeffs: `list` of `float`
       The four P1 coefficients.
    """
    mP1 = p2Coeffs[0] / p2Coeffs[2]
    cosTheta = np.cos(np.arctan(mP1))
    sinTheta = np.sin(np.arctan(mP1))
    deltaP1 = -cosTheta * x0 - sinTheta * y0
    p1Coeffs = [cosTheta, sinTheta - cosTheta, -sinTheta, deltaP1]

    return p1Coeffs


def p2p1CoeffsFromLinearFit(m, b, x0, y0):
    """Derive the Ivezic et al. 2004 P2 and P1 equations based on linear fit
    Where the linear fit is to the given region in color-color space.
    Reference: Ivezic et al. 2004 (2004AN....325..583I)
    For y = m*x + b fit, where x = c1 - c2 and y = c2 - c3,
    P2 = (-m*c1 + (m + 1)*c2 - c3 - b)/sqrt(m**2 + 1)
    P2norm = P2/sqrt[(m**2 + (m + 1)**2 + 1**2)/(m**2 + 1)]
    P1 = cos(theta)*x + sin(theta)*y + deltaP1, theta = arctan(m)
    P1 = cos(theta)*(c1 - c2) + sin(theta)*(c2 - c3) + deltaP1
    P1 = cos(theta)*c1 + ((sin(theta) - cos(theta))*c2 - sin(theta)*c3 + deltaP1
    P1 = 0 at x0, y0 ==> deltaP1 = -cos(theta)*x0 - sin(theta)*y0
    Parameters
    ----------
    m : `float`
       Slope of line to convert.
    b : `float`
       Intercept of line to convert.
    x0, y0 : `float`
       Coordinates at which to set P1 = 0.
    Returns
    -------
    result : `lsst.pipe.base.Struct`
       Result struct with components:
       - ``p2Coeffs`` : four P2 equation coefficents (`list` of `float`).
       - ``p1Coeffs`` : four P1 equation coefficents (`list` of `float`).
    """
    # Compute Ivezic P2 coefficients using the linear fit slope and intercept
    scaleFact = np.sqrt(m ** 2 + 1.0)
    p2Coeffs = [-m / scaleFact, (m + 1.0) / scaleFact, -1.0 / scaleFact, -b / scaleFact]
    p2Norm = 0.0
    for coeff in p2Coeffs[:-1]:  # Omit the constant normalization term
        p2Norm += coeff ** 2
    p2Norm = np.sqrt(p2Norm)
    p2Coeffs /= p2Norm

    # Compute Ivezic P1 coefficients equation using the linear fit slope and
    # point (x0, y0) as the origin
    p1Coeffs = p1CoeffsFromP2x0y0(p2Coeffs, x0, y0)

    return Struct(p2Coeffs=p2Coeffs, p1Coeffs=p1Coeffs,)


def calcQuartileClippedStats(dataArray, nSigmaToClip=3.0):
    """Calculate the quartile-based clipped statistics of a data array.
    The difference between quartiles[2] and quartiles[0] is the interquartile
    distance.  0.74*interquartileDistance is an estimate of standard deviation
    so, in the case that ``dataArray`` has an approximately Gaussian
    distribution, this is equivalent to nSigma clipping.
    Parameters
    ----------
    dataArray : `list` or `numpy.ndarray` of `float`
        List or array containing the values for which the quartile-based
        clipped statistics are to be calculated.
    nSigmaToClip : `float`, optional
        Number of \"sigma\" outside of which to clip data when computing the
        statistics.
    Returns
    -------
    result : `lsst.pipe.base.Struct`
        The quartile-based clipped statistics with ``nSigmaToClip`` clipping.
        Atributes are:
        ``median``
            The median of the full ``dataArray`` (`float`).
        ``mean``
            The quartile-based clipped mean (`float`).
        ``stdDev``
            The quartile-based clipped standard deviation (`float`).
        ``rms``
            The quartile-based clipped root-mean-squared (`float`).
        ``clipValue``
            The value outside of which to clip the data before computing the
            statistics (`float`).
        ``goodArray``
            A boolean array indicating which data points in ``dataArray`` were
            used in the calculation of the statistics, where `False` indicates
            a clipped datapoint (`numpy.ndarray` of `bool`).
    """
    quartiles = np.percentile(dataArray, [25, 50, 75])
    assert len(quartiles) == 3
    median = quartiles[1]
    interQuartileDistance = quartiles[2] - quartiles[0]
    clipValue = nSigmaToClip * 0.74 * interQuartileDistance
    good = np.logical_not(np.abs(dataArray - median) > clipValue)
    quartileClippedMean = dataArray[good].mean()
    quartileClippedStdDev = dataArray[good].std()
    quartileClippedRms = np.sqrt(np.mean(dataArray[good] ** 2))

    return Struct(
        median=median,
        mean=quartileClippedMean,
        stdDev=quartileClippedStdDev,
        rms=quartileClippedRms,
        clipValue=clipValue,
        goodArray=good,
    )
