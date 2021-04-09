import astropy.units as u
import numpy as np

from lsst.pipe.base import Struct, Task
from lsst.verify import Measurement, Datum
from lsst.pex.config import Config, Field
from lsst.faro.utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats
from lsst.faro.utils.matcher import make_matched_photom
from lsst.faro.utils.extinction_corr import extinction_corr
from lsst.faro.utils.tex import calculate_tex

__all__ = ("WPerpTaskConfig", "WPerpTask", "TExTaskConfig", "TExTask")


class WPerpTaskConfig(Config):
    # These are cuts to apply to the r-band only:
    bright_rmag_cut = Field(doc="Bright limit of catalog entries to include",
                            dtype=float, default=17.0)
    faint_rmag_cut = Field(doc="Faint limit of catalog entries to include",
                           dtype=float, default=23.0)


class WPerpTask(Task):
    ConfigClass = WPerpTaskConfig
    _DefaultName = "WPerpTask"

    def run(self, catalogs, photo_calibs, metric_name, dataIds):
        self.log.info(f"Measuring {metric_name}")
        bands = set([f['band'] for f in dataIds])

        if ('g' in bands) & ('r' in bands) & ('i' in bands):
            rgicat_all = make_matched_photom(dataIds, catalogs, photo_calibs)
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


class TExTaskConfig(Config):
    min_sep = Field(doc="Inner radius of the annulus in arcmin",
                    dtype=float, default=0.25)
    max_sep = Field(doc="Outer radius of the annulus in arcmin",
                    dtype=float, default=1.)
    nbins = Field(doc="Number of log-spaced angular bins",
                  dtype=int, default=10)
    rho_stat = Field(doc="Rho statistic to be computed",
                     dtype=int, default=1)
    ellipticity_convention = Field(doc="Ellipticity convention to use (distortion or shear)",
                                   dtype=str, default="distortion")
    column_psf = Field(doc="Column to use for PSF model shape moments",
                       dtype=str, default='slot_PsfShape')
    column = Field(doc="Column to use for shape moments",
                   dtype=str, default='slot_Shape')
    # Eventually want to add option to use only PSF reserve stars


class TExTask(Task):
    ConfigClass = TExTaskConfig
    _DefaultName = "TExTask"

    def run(self, metric_name, catalogs, photo_calibs, astrom_calibs, data_ids):
        self.log.info(f"Measuring {metric_name}")

        result = calculate_tex(catalogs, photo_calibs, astrom_calibs, self.config)

        write_extras = True
        if write_extras:
            extras = {}
            extras['radius'] = Datum(result['radius'], label='radius',
                                     description='Separation (arcmin).')
            extras['corr'] = Datum(result['corr'], label='Correlation',
                                   description='Correlation.')
            extras['corr_err'] = Datum(result['corr_err'], label='Correlation Uncertianty',
                                       description='Correlation Uncertainty.')
        else:
            extras=None
        return Struct(measurement=Measurement(metric_name, np.mean(np.abs(result['corr'])), extras=extras))
