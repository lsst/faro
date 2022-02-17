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
import scipy.stats as scipyStats
import scipy.odr as scipyODR
from lsst.pipe.base import Struct

__all__ = (
    "stellarLocusFit",
    "perpDistance",
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
    ### UPDATE THIS TO CALCULATE FOR ALL COLORS RATHER THAN BETWEEN XMIN/YMIN - XMAX/YMAX!
    xMinLine = np.min(xs)-0.2
    xMaxLine = np.max(xs)+0.2
    yMinLine = np.min(ys)-0.2
    yMinLine = np.max(ys)+0.2

    if np.fabs(paramsOut["mHW"]) > 1:
        ysFitLineHW = np.array([yMinLIne, yMaxLine])
        xsFitLineHW = (ysFitLineHW - paramsOut["bHW"])/paramsOut["mHW"]
        ysFitLine = np.array([yMinLIne, yMaxLine])
        xsFitLine = (ysFitLine - paramsOut["bODR"])/paramsOut["mODR"]
        # "p1" is the position along the stellar locus.
        # Evaluate p1 for all input points:
        p1fit1 = (ys - paramsOut["bODR"])/paramsOut["mODR"]
        ysFitLine2 = np.array([yMinLIne, yMaxLine])
        xsFitLine2 = (ysFitLine2 - paramsOut["bODR2"])/paramsOut["mODR2"]
        p1fit2 = (ys - paramsOut["bODR2"])/paramsOut["mODR2"]
    else:
        xsFitLineHW = np.array([xMinLine, xMaxLine])
        ysFitLineHW = paramsOut["mHW"]*xsFitLineHW + paramsOut["bHW"]
        xsFitLine = np.array([xMinLine, xMaxLine])
        ysFitLine = [paramsOut["mODR"]*xsFitLine[0] + paramsOut["bODR"],
                     paramsOut["mODR"]*xsFitLine[1] + paramsOut["bODR"]]
        p1fit1 = [paramsOut["mODR"]*xs + paramsOut["bODR"]]
        xsFitLine2 = np.array([xMinLine, xMaxLine])
        ysFitLine2 = [paramsOut["mODR2"]*xsFitLine2[0] + paramsOut["bODR2"],
                      paramsOut["mODR2"]*xsFitLine2[1] + paramsOut["bODR2"]]
        p1fit2 = [paramsOut["mODR2"]*xs + paramsOut["bODR2"]]

    # Calculate the distances to the fit line
    # Need two points to characterise the lines we want
    # to get the distances to
    pt1 = np.array([xsFitLine2[0], ysFitLine2[0]])
    pt2 = np.array([xsFitLine2[1], ysFitLine2[1]])

    pt1HW = np.array([xsFitLine2[0], ysFitLineHW[0]])
    pt2HW = np.array([xsFitLine2[1], ysFitLineHW[1]])

    # "p2" is the distance from the fit line
    p2HW = perpDistance(pt1HW, pt2HW, zip(xs, ys))
    p2 = perpDistance(pt1, pt2, zip(xs, ys))

    return np.array(p1fit2), np.array(p2), paramsOut


def perpDistance(pt1, pt2, points):
    """Calculate the perpendicular distance to a line from a point
    Parameters
    ----------
    pt1 : `numpy.ndarray`
         A point on the line
    pt2 : `numpy.ndarray`
         Another point on the line
    points : `zip`
        The points to calculate the distance to
    Returns
    -------
    dists : `list`
        The distances from the line to the points. Uses the cross
        product to work this out.
    """
    dists = []
    for point in points:
        point = np.array(point)
        distToLine = np.cross(pt1 - point, pt2 - point)/np.linalg.norm(pt2 - point)
        dists.append(distToLine)

    return dists
