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

import logging
from astropy.coordinates import SkyCoord

log = logging.getLogger(__name__)

try:
    from dustmaps.sfd import SFDQuery
except ModuleNotFoundError as e:
    log.debug(
        "The extinction_corr method is not available without first installing the dustmaps module:\n"
        "$> pip install --user dustmaps\n\n"
        "Then in a python interpreter:\n"
        ">>> import dustmaps.sfd\n"
        ">>> dustmaps.sfd.fetch()\n"
        "%s",
        e.msg,
    )

__all__ = ("extinction_corr", "extinctionCorrTable",)


def extinctionCorrTable(coords, bands):

    # Extinction coefficients for HSC filters for conversion from E(B-V) to extinction, A_filter.
    # Numbers provided by Masayuki Tanaka (NAOJ).
    #
    # Band, A_filter/E(B-V)
    extinctionCoeffs_HSC = {
        # See https://www.sdss.org/dr16/spectro/sspp/
        # Assuming diff of ~0.65, given 0.553, 0.475, 0.453 for gri
        "u": 4.505,
        "g": 3.240,
        "r": 2.276,
        "i": 1.633,
        "z": 1.263,
        "y": 1.075,
        "HSC-G": 3.240,
        "HSC-R": 2.276,
        "HSC-I": 1.633,
        "HSC-Z": 1.263,
        "HSC-Y": 1.075,
        "NB0387": 4.007,
        "NB0816": 1.458,
        "NB0921": 1.187,
    }

    bands = list(bands)
    sfd = SFDQuery()
    ebvValues = sfd(coords)
    extinction_dict = {"E(B-V)": ebvValues}

    # Create a dict with the extinction values for each band (and E(B-V), too):
    for band in bands:
        coeff_name = "A_" + str(band)
        extinction_dict[coeff_name] = ebvValues * extinctionCoeffs_HSC[band]

    return extinction_dict


def extinction_corr(catalog, bands):

    # Extinction coefficients for HSC filters for conversion from E(B-V) to extinction, A_filter.
    # Numbers provided by Masayuki Tanaka (NAOJ).
    #
    # Band, A_filter/E(B-V)
    extinctionCoeffs_HSC = {
        # See https://www.sdss.org/dr16/spectro/sspp/
        # Assuming diff of ~0.65, given 0.553, 0.475, 0.453 for gri
        "u": 4.505,
        "g": 3.240,
        "r": 2.276,
        "i": 1.633,
        "z": 1.263,
        "y": 1.075,
        "HSC-G": 3.240,
        "HSC-R": 2.276,
        "HSC-I": 1.633,
        "HSC-Z": 1.263,
        "HSC-Y": 1.075,
        "NB0387": 4.007,
        "NB0816": 1.458,
        "NB0921": 1.187,
    }

    bands = list(bands)
    sfd = SFDQuery()
    coord_string_ra = "coord_ra_" + str(bands[0])
    coord_string_dec = "coord_dec_" + str(bands[0])
    coords = SkyCoord(catalog[coord_string_ra], catalog[coord_string_dec])
    ebvValues = sfd(coords)
    extinction_dict = {"E(B-V)": ebvValues}

    # Create a dict with the extinction values for each band (and E(B-V), too):
    for band in bands:
        coeff_name = "A_" + str(band)
        extinction_dict[coeff_name] = ebvValues * extinctionCoeffs_HSC[band]

    return extinction_dict
