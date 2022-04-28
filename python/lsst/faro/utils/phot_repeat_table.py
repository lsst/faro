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
import astropy.units as u

__all__ = ("calcPhotRepeatTable")


def calcPhotRepeatTable(catalog,
                        objectidColumn,
                        fluxColumn,
                        fluxErrColumn,
                        residuals=False):
    """Calculate the photometric repeatability of a set of measurements.

    Parameters
    ----------
    catalog : `pandas.DataFrame` of sources matched between visits using
              objectidColumn as an index
    objectidColumn : `pandas.DataFrame` schema key
        column name for matched object Id column
    fluxColumn : `pandas.DataFrame` schema key
        column name for flux mesurements
    fluxErrColumn : `pandas.DataFrame` schema key
        column name for error on flux meausrements
    residuals: boolean controling whether residuals should be returned in output

    Returns
    -------
    photRepeatFrame : `pandas.DataFrame`
        Repeatability statistics and ancillary quantities calculated from the
        input catalog. Fields are:
        - ``count``: array of number of nonzero magnitude measurements for each
          input source.
        - ``magMean``: `~astropy.unit.Quantity` array of mean magnitudes, in mag,
          for each input source.
        - ``rms``: `~astropy.unit.Quantity` array of RMS photometric repeatability
          about the mean, in mmag, for each input source.
        - ``residuals``:  array for each input source,
          containing the magnitude residuals, in mag, with respect to ``magMean``.
    """
    magColumn = fluxColumn.replace("Flux", "Mag")
    mags = (catalog.loc[:, fluxColumn].values*u.nJy).to(u.ABmag).value
    catalog = catalog.assign(**{magColumn: mags})

    photRepeatFrame = catalog.groupby(
        objectidColumn
    ).aggregate({magColumn: [np.nanstd, np.count_nonzero, np.mean]}).rename(
        columns={
            'nanstd': 'rms',
            'count_nonzero': 'count',
            "mean": "magMean"
        }
    )
    photRepeatFrame.columns = photRepeatFrame.columns.get_level_values(1)
    photRepeatFrame.loc[:, "meanSnr"] = catalog.groupby(
        objectidColumn
    ).apply(
        lambda row: np.mean(row[fluxColumn]/row[fluxErrColumn])
    )
    if residuals:
        photRepeatFrame.loc[:, "residuals"] = catalog.groupby(
            objectidColumn
        )[magColumn].apply(lambda x: np.array(x-np.mean(x)))
    return photRepeatFrame
