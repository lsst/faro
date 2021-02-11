import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum
from lsst.pex.config import Config, Field
from lsst.faro.utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats
from lsst.faro.utils.matcher import make_matched_photom
from lsst.faro.utils.extinction_corr import extinction_corr


class WPerpTaskConfig(Config):
    # These are cuts to apply to the r-band only:
    bright_rmag_cut = Field(doc="Bright limit of catalog entries to include",
                            dtype=float, default=17.0)
    faint_rmag_cut = Field(doc="Faint limit of catalog entries to include",
                           dtype=float, default=23.0)


class WPerpTask(Task):
    ConfigClass = WPerpTaskConfig
    _DefaultName = "WPerpTask"

    def run(self, catalogs, photo_calibs, metric_name, vIds):
        self.log.info(f"Measuring {metric_name}")
        bands = set([f['band'] for f in vIds])

        if ('g' in bands) & ('r' in bands) & ('i' in bands):
            rgicat_all = make_matched_photom(vIds, catalogs, photo_calibs)
            magcut = ((rgicat_all['base_PsfFlux_mag_r'] < self.config.faint_rmag_cut)
                      & (rgicat_all['base_PsfFlux_mag_r'] > self.config.bright_rmag_cut))
            rgicat = rgicat_all[magcut]
            ext_vals = extinction_corr(rgicat, bands)

            wPerp = self.calc_wPerp(rgicat, ext_vals, metric_name)
            return wPerp
        else:
            return Struct(measurement=Measurement(metric_name, np.nan*u.mmag))

    def calc_wPerp(self, phot, extinction_vals, metric_name):
        p1, p2, p1coeffs, p2coeffs = stellarLocusResid(phot['base_PsfFlux_mag_g']-extinction_vals['A_g'],
                                                       phot['base_PsfFlux_mag_r']-extinction_vals['A_r'],
                                                       phot['base_PsfFlux_mag_i']-extinction_vals['A_i'])

        if np.size(p2) > 2:
            p2_rms = calcQuartileClippedStats(p2).rms*u.mag
            extras = {'p1_coeffs': Datum(p1coeffs*u.Unit(''), label='p1_coefficients',
                                         description='p1 coeffs from wPerp fit'),
                      'p2_coeffs': Datum(p2coeffs*u.Unit(''), label='p2_coefficients',
                                         description='p2_coeffs from wPerp fit')}

            return Struct(measurement=Measurement(metric_name, p2_rms.to(u.mmag), extras=extras))
        else:
            return Struct(measurement=Measurement(metric_name, np.nan*u.mmag))
