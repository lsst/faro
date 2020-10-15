import astropy.units as u
from astropy.table import join
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement
from lsst.pex.config import Config, Field, ListField
from metric_pipeline_utils.stellar_locus import stellarLocusResid, calcQuartileClippedStats


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
        filters = set([f['abstract_filter'] for f in vIds])

        if ('g' in filters) & ('r' in filters) & ('i' in filters):
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
                    gcat.extend(catalogs[i].copy(deep=True))
                    gmag0 = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                    gmags.append(gmag0[:, 0])
                    gmag_errs.append(gmag0[:, 1])
                elif (vIds[i]['abstract_filter'] in 'r'):
                    rcat.extend(catalogs[i].copy(deep=True))
                    rmag0 = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                    rmags.append(rmag0[:, 0])
                    rmag_errs.append(rmag0[:, 1])
                elif (vIds[i]['abstract_filter'] in 'i'):
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

# NEXT:
###    - apply photocalib to extract mags
#    - apply external photocalib (FGCM)
#    - apply extinction correction
            # rmag0 = photo_calibs[0].instFluxToMagnitude(catalogs[0], 'base_PsfFlux')
            # rmags = rmag0[:,0]

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
            #   '_r' appended to column names. Ones with nothing appended are i-band.
            p1, p2, p1coeffs, p2coeffs = stellarLocusResid(rgicat['base_PsfFlux_mag_g'],
                                                           rgicat['base_PsfFlux_mag_r'],
                                                           rgicat['base_PsfFlux_mag'])
            import pdb; pdb.set_trace()

#            p1, p2, p1coeffs, p2coeffs = stellarLocusResid(gcat_final[qual_cuts], rcat_final[qual_cuts],
#                                                           icat_final[qual_cuts])

            # gcat_final = gcat[qual_cuts].copy(deep=True)
            # rcat_final = rcat[qual_cuts].copy(deep=True)
            # icat_final = icat[qual_cuts].copy(deep=True)
            if np.size(p2) > 2:
                # p2_rms = np.sqrt(np.mean((p2-np.mean(p2))**2))
                p2_rms = calcQuartileClippedStats(p2).rms*u.mag
                return Struct(measurement=Measurement(metric_name, p2_rms.to(u.mmag)))
                # return Struct(measurement=Measurement(metric_name, (p2_rms*1000.0)*u.mmag))
            else:
                return Struct(measurement=Measurement(metric_name, np.nan*u.mmag))

#        filteredCat = filterMatches(matchedCatalogMultiTract)
#        magRange = np.array([self.config.bright_mag_cut, self.config.faint_mag_cut]) * u.mag
#        minMag, maxMag = magRange.to(u.mag).value
#
#        def magInRange(cat):
#            mag = cat.get('base_PsfFlux_mag')
#            w, = np.where(np.isfinite(mag))
#            medianMag = np.median(mag[w])
#            return minMag <= medianMag and medianMag < maxMag
#
#        groupViewInMagRange = filteredCat.where(magInRange)
#
#        if len(groupViewInMagRange) > 0:
#            p1, p2, p1coeffs, p2coeffs = stellarLocusResid(groupViewInMagRange)
#            if np.size(p2) > 2:
#                p2_rms = np.sqrt(np.mean((p2-np.mean(p2))**2))
#                return Struct(measurement=Measurement(metric_name, p2_rms*u.mag))
#            else:
#                return Struct(measurement=Measurement(metric_name, np.nan*u.mag))
#        else:
#            return Struct(measurement=Measurement(metric_name, np.nan*u.mag))
