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

import numpy as np
import scipy.odr as scipyODR

__all__ = (
    "stellarLocusFit",
    "parallelPerpDistance",
)


def stellarLocusFit(xs, ys, paramDict):
    """Make a fit to the stellar locus
    Parameters
    ----------
    xs : `numpy.ndarray`
        The color on the xaxis
    ys : `numpy.ndarray`
        The color on the yaxis
    paramDict : lsst.pex.config.dictField.Dict
        A dictionary of parameters for line fitting
        xMin : `float`
            The minimum x edge of the box to use for initial fitting
        xMax : `float`
            The maximum x edge of the box to use for initial fitting
        yMin : `float`
            The minimum y edge of the box to use for initial fitting
        yMax : `float`
            The maximum y edge of the box to use for initial fitting
        mHW : `float`
            The hardwired gradient for the fit
        bHW : `float`
            The hardwired intercept of the fit
    Returns
    -------
    paramsOut : `dict`
        A dictionary of the calculated fit parameters
        xMin : `float`
            The minimum x edge of the box to use for initial fitting
        xMax : `float`
            The maximum x edge of the box to use for initial fitting
        yMin : `float`
            The minimum y edge of the box to use for initial fitting
        yMax : `float`
            The maximum y edge of the box to use for initial fitting
        mHW : `float`
            The hardwired gradient for the fit
        bHW : `float`
            The hardwired intercept of the fit
        mODR : `float`
            The gradient calculated by the ODR fit
        bODR : `float`
            The intercept calculated by the ODR fit
        yBoxMin : `float`
            The y value of the fitted line at xMin
        yBoxMax : `float`
            The y value of the fitted line at xMax
        bPerpMin : `float`
            The intercept of the perpendicular line that goes through xMin
        bPerpMax : `float`
            The intercept of the perpendicular line that goes through xMax
        mODR2 : `float`
            The gradient from the second round of fitting
        bODR2 : `float`
            The intercept from the second round of fitting
        mPerp : `float`
            The gradient of the line perpendicular to the line from the
            second fit
    Notes
    -----
    The code does two rounds of fitting, the first is initiated using the
    hardwired values given in the `paramDict` parameter and is done using
    an Orthogonal Distance Regression fit to the points defined by the
    box of xMin, xMax, yMin and yMax. Once this fitting has been done a
    perpendicular bisector is calculated at either end of the line and
    only points that fall within these lines are used to recalculate the fit.
    """

    # Points to use for the fit
    fitPoints = np.where((xs > paramDict["xMin"]) & (xs < paramDict["xMax"])
                         & (ys > paramDict["yMin"]) & (ys < paramDict["yMax"]))[0]

    linear = scipyODR.polynomial(1)

    data = scipyODR.Data(xs[fitPoints], ys[fitPoints])
    odr = scipyODR.ODR(data, linear, beta0=[paramDict["bHW"], paramDict["mHW"]])
    params = odr.run()
    mODR = float(params.beta[1])
    bODR = float(params.beta[0])

    paramsOut = {"xMin": paramDict["xMin"], "xMax": paramDict["xMax"], "yMin": paramDict["yMin"],
                 "yMax": paramDict["yMax"], "mHW": paramDict["mHW"], "bHW": paramDict["bHW"],
                 "mODR": mODR, "bODR": bODR}

    # Having found the initial fit calculate perpendicular ends
    mPerp = -1.0/mODR
    # When the gradient is really steep we need to use
    # the y limits of the box rather than the x ones

    if np.fabs(mODR) > 1:
        yBoxMin = paramDict["yMin"]
        xBoxMin = (yBoxMin - bODR)/mODR
        yBoxMax = paramDict["yMax"]
        xBoxMax = (yBoxMax - bODR)/mODR
    else:
        yBoxMin = mODR*paramDict["xMin"] + bODR
        xBoxMin = paramDict["xMin"]
        yBoxMax = mODR*paramDict["xMax"] + bODR
        xBoxMax = paramDict["xMax"]

    bPerpMin = yBoxMin - mPerp*xBoxMin

    paramsOut["yBoxMin"] = yBoxMin
    paramsOut["bPerpMin"] = bPerpMin

    bPerpMax = yBoxMax - mPerp*xBoxMax

    paramsOut["yBoxMax"] = yBoxMax
    paramsOut["bPerpMax"] = bPerpMax

    # Use these perpendicular lines to chose the data and refit
    fitPoints = ((ys > mPerp*xs + bPerpMin) & (ys < mPerp*xs + bPerpMax))
    data = scipyODR.Data(xs[fitPoints], ys[fitPoints])
    odr = scipyODR.ODR(data, linear, beta0=[bODR, mODR])
    params = odr.run()
    mODR = float(params.beta[1])
    bODR = float(params.beta[0])

    paramsOut["mODR2"] = float(params.beta[1])
    paramsOut["bODR2"] = float(params.beta[0])

    paramsOut["mPerp"] = -1.0/paramsOut["mODR2"]

    # Calculate the fit lines for the entire X, Y range
    xMinLine = np.min(xs)-0.2
    xMaxLine = np.max(xs)+0.2
    yMinLine = np.min(ys)-0.2
    yMaxLine = np.max(ys)+0.2

    if np.fabs(paramsOut["mHW"]) > 1:
        ysFitLineHW = np.array([yMinLine, yMaxLine])
        xsFitLineHW = (ysFitLineHW - paramsOut["bHW"])/paramsOut["mHW"]
        yFiducialHW = np.array(paramsOut['yMin'])
        xFiducialHW = (yFiducialHW - paramsOut["bODR"])/paramsOut["mODR"]
        ysFitLine = np.array([yMinLine, yMaxLine])
        xsFitLine = (ysFitLine - paramsOut["bODR"])/paramsOut["mODR"]
        ysFitLine2 = np.array([yMinLine, yMaxLine])
        xsFitLine2 = (ysFitLine2 - paramsOut["bODR2"])/paramsOut["mODR2"]
        yFiducial = np.array(paramsOut['yMin'])
        xFiducial = (yFiducial - paramsOut["bODR2"])/paramsOut["mODR2"]
    else:
        xsFitLineHW = np.array([xMinLine, xMaxLine])
        ysFitLineHW = paramsOut["mHW"]*xsFitLineHW + paramsOut["bHW"]
        xFiducialHW = np.array(paramsOut['xMin'])
        yFiducialHW = paramsOut["mHW"]*xFiducialHW + paramsOut["bHW"]
        xsFitLine = np.array([xMinLine, xMaxLine])
        ysFitLine = [paramsOut["mODR"]*xsFitLine[0] + paramsOut["bODR"],
                     paramsOut["mODR"]*xsFitLine[1] + paramsOut["bODR"]]
        xsFitLine2 = np.array([xMinLine, xMaxLine])
        ysFitLine2 = [paramsOut["mODR2"]*xsFitLine2[0] + paramsOut["bODR2"],
                      paramsOut["mODR2"]*xsFitLine2[1] + paramsOut["bODR2"]]
        xFiducial = np.array(paramsOut['xMin'])
        yFiducial = paramsOut["mODR2"]*xFiducial + paramsOut["bODR2"]

    # Calculate the distances to the fit line
    # Need two points to characterise the lines we want
    # to get the distances to
    pt1 = np.array([xFiducial, yFiducial])
    pt2 = np.array([xsFitLine2[1], ysFitLine2[1]])

    pt1HW = np.array([xFiducialHW, yFiducialHW])
    pt2HW = np.array([xsFitLine2[1], ysFitLineHW[1]])

    # "p2" is the distance from the fit line, and "p1" is the distance
    # along the line.
    p1HW, p2HW = parallelPerpDistance(pt1HW, pt2HW, zip(xs, ys))
    p1, p2 = parallelPerpDistance(pt1, pt2, zip(xs, ys))

    return np.array(p1), np.array(p2), paramsOut


def parallelPerpDistance(pt1, pt2, points):
    """Calculate the perpendicular distance to a line from a point, and
       the distance along the line (parallel) from that point to a fiducial.
    Parameters
    ----------
    pt1 : `numpy.ndarray`
        A point on the line; this will be taken as the zero point for
        the parallel distance.
    pt2 : `numpy.ndarray`
        Another point on the line
    points : `zip`
        The points to calculate the distance to
    Returns
    -------
    dists_parallel : `list`
        The distances along the line from the points to the fiducial point (pt1).
    dists_perp : `list`
        The distances from the line to the points. Uses the cross
        product to work this out.
    """
    dists_parallel = []
    dists_perp = []

    for point in points:
        point = np.array(point)
        distToLine = np.cross(pt1 - point, pt2 - point)/np.linalg.norm(pt2 - pt1)
        distAlongLine = np.sqrt((np.linalg.norm(pt1 - point))**2 - distToLine**2)
        sign = np.sign((point - pt1)/np.linalg.norm(point - pt1))[0]
        dists_perp.append(distToLine)
        dists_parallel.append(sign*distAlongLine)

    return dists_parallel, dists_perp
