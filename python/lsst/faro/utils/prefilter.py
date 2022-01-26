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

__all__ = ("preFilter",)


def preFilter(
    sourceCatalog,
    snrMin=None,
    snrMax=None,
    brightMagCut=None,
    faintMagCut=None,
    extended=None,
    doFlags=None,
    isPrimary=None,
    psfStars=None,
    photoCalibStars=None,
    astromCalibStars=None,
):

    if snrMin is None:
        snrMin = 50.0
    if snrMax is None:
        snrMax = np.Inf
    if extended is None:
        extended = False

    def filtSNR(cat):
        oksnr = (cat["base_PsfFlux_snr"] > snrMin) & (cat["base_PsfFlux_snr"] < snrMax)
        return oksnr

    def filtMag(cat):
        if (brightMagCut is not None) and (faintMagCut is not None):
            okmag = (cat["base_PsfFlux_mag"] > brightMagCut) & (cat["base_PsfFlux_mag"] < faintMagCut)
        else:
            okmag = (cat["base_PsfFlux_mag"] < 30.0)
        return okmag

    def filtExtended(cat):
        if extended:
            sourceType = (cat["base_ClassificationExtendedness_value"] > 0.9)
        else:
            sourceType = (cat["base_ClassificationExtendedness_value"] < 0.1)
        return sourceType

    def filtFlags(cat):
        flag_sat = ~cat["base_PixelFlags_flag_saturated"]
        flag_cr = ~cat["base_PixelFlags_flag_cr"]
        flag_bad = ~cat["base_PixelFlags_flag_bad"]
        flag_edge = ~cat["base_PixelFlags_flag_edge"]
        allflags = flag_sat & flag_cr & flag_bad & flag_edge
        return allflags

    def filtPrimary(cat):
        is_primary = cat["detect_isPrimary"]
        return is_primary

    allfilt = filtSNR(sourceCatalog) & filtExtended(sourceCatalog) &\
        filtFlags(sourceCatalog) & filtPrimary(sourceCatalog) &\
        filtMag(sourceCatalog)

    return sourceCatalog[allfilt].copy(deep=True)
