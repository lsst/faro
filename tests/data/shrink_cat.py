import numpy as np
import sys
import logging

from lsst.afw.table import SimpleCatalog

required_columns = ['slot_PsfFlux_mag', 'base_PsfFlux_snr', 'base_ClassificationExtendedness_value',
                    'id', 'coord_ra', 'coord_dec', 'object', 'visit', 'base_PixelFlags_flag_saturated',
                    'base_PixelFlags_flag_cr', 'base_PixelFlags_flag_bad', 'base_PixelFlags_flag_edge',
                    'base_PsfFlux_snr', 'base_PsfFlux_mag', 'base_PsfFlux_magErr', 'base_GaussianFlux_mag',
                    'base_GaussianFlux_magErr', 'base_GaussianFlux_snr', 'e1', 'e2', 'psf_e1', 'psf_e2',
                    'filt', 'detect_isPrimary']

log = logging.getLogger(__name__)


def shrinkCat(infile, outfile, verbose=False):

    cat = SimpleCatalog.readFits(infile)
    for name in cat.schema.getNames():
        if name in required_columns or 'flag' in name:  # Flags should compress anyway
            continue
        if verbose:
            log.debug(f"Shrinking {name}")
        try:
            cat[name][:] = np.repeat(0, len(cat))
        except ValueError as e:
            log.debug(e.msg)

    cat.writeFits(outfile)


if __name__ == "__main__":
    infile = sys.argv[1]
    outfile = sys.argv[2]
    shrinkCat(infile, outfile)
