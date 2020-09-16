import numpy as np
import scipy.optimize as scipyOptimize
import scipy.stats as scipyStats
import scipy.odr as scipyOdr
from lsst.pipe.base import Struct


def calcP1P2(mags, coeffs):
    # P1 =A′u+B′g+C′r+D′i+E′z+F′
    # P2′=Au+Bg+Cr+Di+Ez+F
    # umag = np.array(mags['umag'],dtype='float')
    # gmag = np.array(mags['gmag'],dtype='float')
    # rmag = np.array(mags['rmag'],dtype='float')
    # imag = np.array(mags['imag'],dtype='float')
    # zmag = np.array(mags['zmag'],dtype='float')
    p1p2 = float(coeffs[0])*mags[0] + float(coeffs[1])*mags[1] +\
        float(coeffs[2])*mags[2] + float(coeffs[3])*mags[3] +\
        float(coeffs[4])*mags[4] + float(coeffs[5])
    return p1p2


# Coefficients from the Ivezic+2004 paper. Warning - if possible, the Coefficients
# should be derived from a fit to the stellar locus rather than these "fallback" values.
ivezicCoeffs = {'P1s': [0.91, -0.495, -0.415, 0.0, 0.0, -1.28],
                'P1w': [0.0, 0.928, -0.556, -0.372, 0.0, -0.425],
                'P2s': [-0.249, 0.794, -0.555, 0.0, 0.0, 0.234],
                'P2w': [0.0, -0.227, 0.792, -0.567, 0.0, 0.050]}
ivezicCoeffsHSC = {'P1s': [0.91, -0.495, -0.415, 0.0, 0.0, -1.28],
                   'P1w': [0.0, 0.888, -0.427, -0.461, 0.0, -0.478],
                   'P2s': [-0.249, 0.794, -0.555, 0.0, 0.0, 0.234],
                   'P2w': [0.0, -0.274, 0.803, -0.529, 0.0, 0.041]}


# Everything below this is copied directly from pipe_analysis/utils.py.
# Should we move all those functions here once pipe_analysis is rewritten?
def orthogonalRegression(x, y, order, initialGuess=None):
    """Perform an Orthogonal Distance Regression on the given data
    Parameters
    ----------
    x, y : `array`
       Arrays of x and y data to fit
    order : `int`, optional
       Order of the polynomial to fit
    initialGuess : `list` of `float`, optional
       List of the polynomial coefficients (highest power first) of an initial guess to feed to
       the ODR fit.  If no initialGuess is provided, a simple linear fit is performed and used
       as the guess (`None` by default).
    Returns
    -------
    result : `list` of `float`
       List of the fit coefficients (highest power first to mimic `numpy.polyfit` return).
    """
    if initialGuess is None:
        linReg = scipyStats.linregress(x, y)
        initialGuess = [linReg[0], linReg[1]]
        for i in range(order - 1):  # initialGuess here is linear, so need to pad array to match order
            initialGuess.insert(0, 0.0)
    if order == 1:
        odrModel = scipyOdr.Model(fLinear)
    elif order == 2:
        odrModel = scipyOdr.Model(fQuadratic)
    elif order == 3:
        odrModel = scipyOdr.Model(fCubic)
    else:
        raise RuntimeError("Order must be between 1 and 3 (value requested, {:}, not accommodated)".
                           format(order))
    odrData = scipyOdr.Data(x, y)
    orthDist = scipyOdr.ODR(odrData, odrModel, beta0=initialGuess)
    orthRegFit = orthDist.run()

    return list(reversed(orthRegFit.beta))


def distanceSquaredToPoly(x1, y1, x2, poly):
    """Calculate the square of the distance between point (x1, y1) and poly at x2
    Parameters
    ----------
    x1, y1 : `float`
       Point from which to calculate the square of the distance to the
       polynomial ``poly``.
    x2 : `float`
       Position on x axis from which to calculate the square of the distance
       between (``x1``, ``y1``) and ``poly`` (the position of the tangent of
       the polynomial curve closest to point (``x1``, ``y1``)).
    poly : `numpy.lib.polynomial.poly1d`
       Numpy polynomial fit from which to calculate the square of the distance
       to (``x1``, ``y1``) at ``x2``.
    Returns
    -------
    result : `float`
       Square of the distance between (``x1``, ``y1``) and ``poly`` at ``x2``
    """
    return (x2 - x1)**2 + (poly(x2) - y1)**2


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
    mP1 = p2Coeffs[0]/p2Coeffs[2]
    cosTheta = np.cos(np.arctan(mP1))
    sinTheta = np.sin(np.arctan(mP1))
    deltaP1 = -cosTheta*x0 - sinTheta*y0
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
    scaleFact = np.sqrt(m**2 + 1.0)
    p2Coeffs = [-m/scaleFact, (m + 1.0)/scaleFact, -1.0/scaleFact, -b/scaleFact]
    p2Norm = 0.0
    for coeff in p2Coeffs[:-1]:  # Omit the constant normalization term
        p2Norm += coeff**2
    p2Norm = np.sqrt(p2Norm)
    p2Coeffs /= p2Norm

    # Compute Ivezic P1 coefficients equation using the linear fit slope and
    # point (x0, y0) as the origin
    p1Coeffs = p1CoeffsFromP2x0y0(p2Coeffs, x0, y0)

    return Struct(
        p2Coeffs=p2Coeffs,
        p1Coeffs=p1Coeffs,
    )


def lineFromP2Coeffs(p2Coeffs):
    """Compute P1 line in color-color space for given set P2 coefficients
    Reference: Ivezic et al. 2004 (2004AN....325..583I)
    Parameters
    ----------
    p2Coeffs : `list` of `float`
       List of the four P2 coefficients.
    Returns
    -------
    result : `lsst.pipe.base.Struct`
       Result struct with components:
       - ``mP1`` : associated slope for P1 in color-color coordinates (`float`).
       - ``bP1`` : associated intercept for P1 in color-color coordinates
                   (`float`).
    """
    mP1 = p2Coeffs[0]/p2Coeffs[2]
    bP1 = -p2Coeffs[3]*np.sqrt(mP1**2 + (mP1 + 1.0)**2 + 1.0)
    return Struct(
        mP1=mP1,
        bP1=bP1,
    )


def linesFromP2P1Coeffs(p2Coeffs, p1Coeffs):
    """Derive P1/P2 axes in color-color space based on the P2 and P1 coeffs
    Reference: Ivezic et al. 2004 (2004AN....325..583I)
    Parameters
    ----------
    p2Coeffs : `list` of `float`
       List of the four P2 coefficients.
    p1Coeffs : `list` of `float`
       List of the four P1 coefficients.
    Returns
    -------
    result : `lsst.pipe.base.Struct`
       Result struct with components:
       - ``mP2``, ``mP1`` : associated slopes for P2 and P1 in color-color
                            coordinates (`float`).
       - ``bP2``, ``bP1`` : associated intercepts for P2 and P1 in color-color
                            coordinates (`float`).
       - ``x0``, ``y0`` : x and y coordinates of the P2/P1 axes origin in
                          color-color coordinates (`float`).
    """
    p1Line = lineFromP2Coeffs(p2Coeffs)
    mP1 = p1Line.mP1
    bP1 = p1Line.bP1

    cosTheta = np.cos(np.arctan(mP1))
    sinTheta = np.sin(np.arctan(mP1))

    def func2(x):
        y = [cosTheta*x[0] + sinTheta*x[1] + p1Coeffs[3], mP1*x[0] - x[1] + bP1]
        return y

    x0y0 = scipyOptimize.fsolve(func2, [1, 1])
    mP2 = -1.0/mP1
    bP2 = x0y0[1] - mP2*x0y0[0]
    return Struct(
        mP2=mP2,
        bP2=bP2,
        mP1=mP1,
        bP1=bP1,
        x0=x0y0[0],
        y0=x0y0[1],
    )
