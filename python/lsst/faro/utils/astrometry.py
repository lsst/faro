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

from astropy.coordinates.angle_utilities import angular_separation


def calculateAA1(catalog, refCat, config):
    """Calculate the astrometric absolute median deviation of a set of measurements.
    This is computed along either an axis (ra or dec) or total angular seperation (total)

    Parameters
    ----------
    catalog : `parquet table`

    refCat : `parquet table`

    config:
        Important parts of config are raColumn,decColumn, refRaColumn,
        refDecColumn and metricColumn


    Returns
    -------
    statistics : `dict`
        astrometric absolute median deviation and ancillary quantities calculated
        from the input catalog and matched refCat. Fields are:
        - ``count``: number of sources used for calculation.
        - ``medianAbsDeviation``: `~astropy.unit.Quantity` Median absolute deviation
            of sources in the catalog and refCat computed using metricColumn in mas
        - ``rms``: `~astropy.unit.Quantity` RMS of positions of sources in the catalog
            and refCat computed using metricColumn in mas.
        - ``center``: scalar `~astropy.unit.Quantity` of the median separation
          of sources in the catalog and refCat computed using metricColumn in mas.
        - ``metricResid``: `~astropy.unit.Quantity` array containing the astrometric
            residuals, in mas, using the metricColumn.
    """
    catDec = np.array(catalog[config._getColumnName("dec")]) * u.deg
    refCatDec = np.array(refCat[config.refColumns["dec"]]) * u.deg

    if (config.metricColumn == "ra") | (config.metricColumn == "total"):
        catRa = np.array(catalog[config._getColumnName("ra")]) * u.deg
        refCatRa = np.array(refCat[config.refColumns["ra"]]) * u.deg

    if config.metricColumn == "dec":
        separations = catDec - refCatDec
    elif config.metricColumn == "ra":
        separations = (catRa - refCatRa) * np.cos((catDec).to(u.radian))
    elif config.metricColumn == "total":
        separations = angular_separation(catRa, catDec, refCatRa, refCatDec)
    else:
        # bad metricColumn specification
        return {"nomeas": np.nan * u.mas}

    # convert separations to mas
    separations = (separations).to(u.mas)

    return {"count": len(separations),
            "medianAbsDeviation": np.median(abs(separations)),
            "rms": np.nanstd(separations),
            "center": np.median(separations),
            "metricResid": separations,
            }
