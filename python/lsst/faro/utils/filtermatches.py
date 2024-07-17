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
from lsst.afw.table import GroupView

__all__ = ("filterMatches",)


def filterMatches(
    matchedCatalog,
    snrMin=None,
    snrMax=None,
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
        snrMax = np.inf
    if extended is None:
        extended = False
    if doFlags is None:
        doFlags = True
    nMatchesRequired = 2
    if isPrimary is None:
        isPrimary = True
    if psfStars is None:
        psfStars = False
    if photoCalibStars is None:
        photoCalibStars = False
    if astromCalibStars is None:
        astromCalibStars = False

    matchedCat = GroupView.build(matchedCatalog)
    magKey = matchedCat.schema.find("slot_PsfFlux_mag").key

    def nMatchFilter(cat):
        if len(cat) < nMatchesRequired:
            return False
        return np.isfinite(cat[magKey]).all()

    def snrFilter(cat):
        # Note that this also implicitly checks for psfSnr being non-nan.
        snr = cat["base_PsfFlux_snr"]
        (ok0,) = np.where(np.isfinite(snr))
        medianSnr = np.median(snr[ok0])
        return snrMin <= medianSnr and medianSnr <= snrMax

    def ptsrcFilter(cat):
        ext = cat["base_ClassificationExtendedness_value"]
        # Keep only objects that are flagged as "not extended" in *ALL* visits,
        # (base_ClassificationExtendedness_value = 1 for extended, 0 for point-like)
        if extended:
            return np.min(ext) > 0.9
        else:
            return np.max(ext) < 0.9

    def flagFilter(cat):
        if doFlags:
            flag_sat = cat["base_PixelFlags_flag_saturated"]
            flag_cr = cat["base_PixelFlags_flag_cr"]
            flag_bad = cat["base_PixelFlags_flag_bad"]
            flag_edge = cat["base_PixelFlags_flag_edge"]
            return np.logical_not(np.any([flag_sat, flag_cr, flag_bad, flag_edge]))
        else:
            return True

    def isPrimaryFilter(cat):
        if isPrimary:
            flag_isPrimary = cat["detect_isPrimary"]
            return np.all(flag_isPrimary)
        else:
            return True

    def fullFilter(cat):
        return (
            nMatchFilter(cat)
            and snrFilter(cat)
            and ptsrcFilter(cat)
            and flagFilter(cat)
            and isPrimaryFilter(cat)
        )

    return matchedCat.where(fullFilter)
