import numpy as np
from lsst.afw.table import GroupView


def filterMatches(matchedCatalog, snrMin=None, snrMax=None,
                  extended=None, doFlags=None, isPrimary=None,
                  psfStars=None, photoCalibStars=None,
                  astromCalibStars=None):

    if snrMin is None:
        snrMin = 50.0
    if snrMax is None:
        snrMax = np.Inf
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
    magKey = matchedCat.schema.find('slot_PsfFlux_mag').key

    def nMatchFilter(cat):
        if len(cat) < nMatchesRequired:
            return False
        return np.isfinite(cat.get(magKey)).all()

    def snrFilter(cat):
        # Note that this also implicitly checks for psfSnr being non-nan.
        snr = cat.get('base_PsfFlux_snr')
        ok0, = np.where(np.isfinite(snr))
        medianSnr = np.median(snr[ok0])
        return snrMin <= medianSnr and medianSnr <= snrMax

    def ptsrcFilter(cat):
        ext = cat.get('base_ClassificationExtendedness_value')
        # Keep only objects that are flagged as "not extended" in *ALL* visits,
        # (base_ClassificationExtendedness_value = 1 for extended, 0 for point-like)
        if extended:
            return np.min(ext) > 0.9
        else:
            return np.max(ext) < 0.9

    def flagFilter(cat):
        if doFlags:
            flag_sat = cat.get("base_PixelFlags_flag_saturated")
            flag_cr = cat.get("base_PixelFlags_flag_cr")
            flag_bad = cat.get("base_PixelFlags_flag_bad")
            flag_edge = cat.get("base_PixelFlags_flag_edge")
            return np.logical_not(np.any([flag_sat, flag_cr, flag_bad, flag_edge]))
        else:
            return True

    def isPrimaryFilter(cat):
        if isPrimary:
            flag_isPrimary = cat.get("detect_isPrimary")
            return np.all(flag_isPrimary)
        else:
            return True

    def fullFilter(cat):
        return nMatchFilter(cat) and snrFilter(cat) and ptsrcFilter(cat)\
            and flagFilter(cat) and isPrimaryFilter(cat)

    return matchedCat.where(fullFilter)
