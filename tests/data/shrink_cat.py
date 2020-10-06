import numpy as np
import sys

from lsst.afw.table import SimpleCatalog

required_columns = ['slot_PsfFlux_mag', 'base_PsfFlux_snr', 'base_ClassificationExtendedness_value',
                    'id', 'coord_ra', 'coord_dec', 'object', 'visit', 'base_PixelFlags_flag_saturated',
                    'base_PixelFlags_flag_cr', 'base_PixelFlags_flag_bad', 'base_PixelFlags_flag_edge',
                    'base_PsfFlux_snr', 'base_PsfFlux_mag', 'base_PsfFlux_magErr', 'base_GaussianFlux_mag',
                    'base_GaussianFlux_magErr', 'base_GaussianFlux_snr', 'e1', 'e2', 'psf_e1', 'psf_e2',
                    'filt']


cat = SimpleCatalog.readFits(sys.argv[1])
for name in cat.schema.getNames():
    if name in required_columns or 'flag' in name:  # Flags should compress anyway
        continue
    print(f"Doing {name}")
    try:
        cat[name][:] = np.repeat(0, len(cat))
    except ValueError as e:
        print(e)

cat.writeFits(sys.argv[2])
