import astropy.units as u
from astropy.table import join
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement, Datum
from lsst.pex.config import Config, Field, ListField
from metric_pipeline_utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats
from lsst.sims.catUtils.dust.EBV import EBVbase as ebv


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
        patches = set([p['patch'] for p in vIds])
        bands = set([f['abstract_filter'] for f in vIds])
        # bands = set([f['band'] for f in vIds])

        # Extinction coefficients for HSC filters for conversion from E(B-V) to extinction, A_filter.                                                                             
        # Numbers provided by Masayuki Tanaka (NAOJ).                                                                                                                             
        #                                                                                                                                                                         
        # Band, A_filter/E(B-V)                                                                                                                                                   
        extinctionCoeffs_HSC = {
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

        if ('g' in bands) & ('r' in bands) & ('i' in bands):
            gcat = catalogs[0].copy()
            gcat.clear()
            rcat = catalogs[0].copy()
            rcat.clear()
            icat = catalogs[0].copy()
            icat.clear()
            gmags = []
            rmags = []
            imags = []
            gmag_errs = []
            rmag_errs = []
            imag_errs = []

            for i in range(len(catalogs)):
                if (vIds[i]['abstract_filter'] in 'g'):
                # if (vIds[i]['band'] in 'g'):
                    gcat.extend(catalogs[i].copy(deep=True))
                    gmag0 = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                    gmags.append(gmag0[:, 0])
                    gmag_errs.append(gmag0[:, 1])
                elif (vIds[i]['abstract_filter'] in 'r'):
                # elif (vIds[i]['band'] in 'r'):
                    rcat.extend(catalogs[i].copy(deep=True))
                    rmag0 = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                    rmags.append(rmag0[:, 0])
                    rmag_errs.append(rmag0[:, 1])
                elif (vIds[i]['abstract_filter'] in 'i'):
                # elif (vIds[i]['band'] in 'i'):
                    icat.extend(catalogs[i].copy(deep=True))
                    imag0 = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                    imags.append(imag0[:, 0])
                    imag_errs.append(imag0[:, 1])

            if not gcat.isContiguous():
                gcat = gcat.copy(deep=True)
                gcat_final = gcat.asAstropy()
                gcat_final['base_PsfFlux_mag'] = np.concatenate(gmags)
                gcat_final['base_PsfFlux_magErr'] = np.concatenate(gmag_errs)
            if not rcat.isContiguous():
                rcat = rcat.copy(deep=True)
                rcat_final = rcat.asAstropy()
                rcat_final['base_PsfFlux_mag'] = np.concatenate(rmags)
                rcat_final['base_PsfFlux_magErr'] = np.concatenate(rmag_errs)
            if not icat.isContiguous():
                icat = icat.copy(deep=True)
                icat_final = icat.asAstropy()
                icat_final['base_PsfFlux_mag'] = np.concatenate(imags)
                icat_final['base_PsfFlux_magErr'] = np.concatenate(imag_errs)

            qual_cuts = (rcat_final['base_ClassificationExtendedness_value'] < 0.5) &\
                        (rcat_final['base_PixelFlags_flag_saturated'] == False) &\
                        (rcat_final['base_PixelFlags_flag_cr'] == False) &\
                        (rcat_final['base_PixelFlags_flag_bad'] == False) &\
                        (rcat_final['base_PixelFlags_flag_edge'] == False) &\
                        (rcat_final['base_PsfFlux_mag'] < self.config.faint_rmag_cut) &\
                        (rcat_final['base_PsfFlux_mag'] > self.config.bright_rmag_cut)

            rgcat = join(rcat_final[qual_cuts], gcat_final, keys='id', table_names=['r', 'g'])
            rgicat = join(rgcat, icat_final, keys='id', table_names=['rg', 'i'])
            # Note that the way this was written, the g- and r-band data will have '_g' and
            #   '_r' appended to column names. Ones with nothing appended are i-band. Rename them:
            rgicat['base_PsfFlux_mag'].name = 'base_PsfFlux_mag_i'
            rgicat['base_PsfFlux_magErr'].name = 'base_PsfFlux_magErr_i'

            ebvObject = ebv()
            ebvValues = ebvObject.calculateEbv(equatorialCoordinates=np.array([rgicat['coord_ra'], rgicat['coord_dec']]))
            A_g = ebvValues*extinctionCoeffs_HSC['HSC-G']
            A_r = ebvValues*extinctionCoeffs_HSC['HSC-R']
            A_i = ebvValues*extinctionCoeffs_HSC['HSC-I']

            p1, p2, p1coeffs, p2coeffs = stellarLocusResid(rgicat['base_PsfFlux_mag_g']-A_g,
                                                           rgicat['base_PsfFlux_mag_r']-A_r,
                                                           rgicat['base_PsfFlux_mag_i']-A_i)

            if np.size(p2) > 2:
                p2_rms = calcQuartileClippedStats(p2).rms*u.mag
                extras = {'p1_coeffs': Datum(p1coeffs*u.Unit(''), label='p1_coefficients', description='p1 coeffs from wPerp fit'),
                          'p2_coeffs': Datum(p2coeffs*u.Unit(''), label='p2_coefficients', description='p2_coeffs from wPerp fit')}
                return Struct(measurement=Measurement(metric_name, p2_rms.to(u.mmag), extras=extras))
            else:
                return Struct(measurement=Measurement(metric_name, np.nan*u.mmag))
