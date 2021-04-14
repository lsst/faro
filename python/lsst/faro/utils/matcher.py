from lsst.afw.table import (SchemaMapper, Field,
                            MultiMatch, SimpleRecord,
                            SourceCatalog, updateSourceCoords)

import numpy as np
from astropy.table import join, Table

__all__ = ("match_catalogs", "ellipticity_from_cat", "ellipticity", "make_matched_photom",
           "mergeCatalogs")


def match_catalogs(inputs, photoCalibs, astromCalibs, vIds, matchRadius,
                   apply_external_wcs=False, logger=None):
    schema = inputs[0].schema
    mapper = SchemaMapper(schema)
    mapper.addMinimalSchema(schema)
    mapper.addOutputField(Field[float]('base_PsfFlux_snr',
                                       'PSF flux SNR'))
    mapper.addOutputField(Field[float]('base_PsfFlux_mag',
                                       'PSF magnitude'))
    mapper.addOutputField(Field[float]('base_PsfFlux_magErr',
                                       'PSF magnitude uncertainty'))
    # Needed because addOutputField(... 'slot_ModelFlux_mag') will add a field with that literal name
    aliasMap = schema.getAliasMap()
    # Possibly not needed since base_GaussianFlux is the default, but this ought to be safe
    modelName = aliasMap['slot_ModelFlux'] if 'slot_ModelFlux' in aliasMap.keys() else 'base_GaussianFlux'
    mapper.addOutputField(Field[float](f'{modelName}_mag',
                                       'Model magnitude'))
    mapper.addOutputField(Field[float](f'{modelName}_magErr',
                                       'Model magnitude uncertainty'))
    mapper.addOutputField(Field[float](f'{modelName}_snr',
                                       'Model flux snr'))
    mapper.addOutputField(Field[float]('e1',
                                       'Source Ellipticity 1'))
    mapper.addOutputField(Field[float]('e2',
                                       'Source Ellipticity 1'))
    mapper.addOutputField(Field[float]('psf_e1',
                                       'PSF Ellipticity 1'))
    mapper.addOutputField(Field[float]('psf_e2',
                                       'PSF Ellipticity 1'))
    mapper.addOutputField(Field[np.int32]('filt',
                                          'filter code'))
    newSchema = mapper.getOutputSchema()
    newSchema.setAliasMap(schema.getAliasMap())

    # Create an object that matches multiple catalogs with same schema
    mmatch = MultiMatch(newSchema,
                        dataIdFormat={'visit': np.int32, 'detector': np.int32},
                        radius=matchRadius,
                        RecordClass=SimpleRecord)

    # create the new extended source catalog
    srcVis = SourceCatalog(newSchema)

    filter_dict = {'u': 1, 'g': 2, 'r': 3, 'i': 4, 'z': 5, 'y': 6,
                   'HSC-U': 1, 'HSC-G': 2, 'HSC-R': 3, 'HSC-I': 4, 'HSC-Z': 5, 'HSC-Y': 6}

    # Sort by visit, detector, then filter
    vislist = [v['visit'] for v in vIds]
    ccdlist = [v['detector'] for v in vIds]
    filtlist = [v['band'] for v in vIds]
    tab_vids = Table([vislist, ccdlist, filtlist], names=['vis', 'ccd', 'filt'])
    sortinds = np.argsort(tab_vids, order=('vis', 'ccd', 'filt'))

    for ind in sortinds:
        oldSrc = inputs[ind]
        photoCalib = photoCalibs[ind]
        wcs = astromCalibs[ind]
        vId = vIds[ind]

        if logger:
            logger.debug(f"{len(oldSrc)} sources in ccd {vId['detector']}  visit {vId['visit']}")

        # create temporary catalog
        tmpCat = SourceCatalog(SourceCatalog(newSchema).table)
        tmpCat.extend(oldSrc, mapper=mapper)

        filtnum = filter_dict[vId['band']]
        tmpCat['filt'] = np.repeat(filtnum, len(oldSrc))

        tmpCat['base_PsfFlux_snr'][:] = tmpCat['base_PsfFlux_instFlux'] \
            / tmpCat['base_PsfFlux_instFluxErr']

        if apply_external_wcs and wcs is not None:
            updateSourceCoords(wcs, tmpCat)

        photoCalib.instFluxToMagnitude(tmpCat, "base_PsfFlux", "base_PsfFlux")
        tmpCat['slot_ModelFlux_snr'][:] = (tmpCat['slot_ModelFlux_instFlux']
                                           / tmpCat['slot_ModelFlux_instFluxErr'])
        photoCalib.instFluxToMagnitude(tmpCat, "slot_ModelFlux", "slot_ModelFlux")

        _, psf_e1, psf_e2 = ellipticity_from_cat(oldSrc, slot_shape='slot_PsfShape')
        _, star_e1, star_e2 = ellipticity_from_cat(oldSrc, slot_shape='slot_Shape')
        tmpCat['e1'][:] = star_e1
        tmpCat['e2'][:] = star_e2
        tmpCat['psf_e1'][:] = psf_e1
        tmpCat['psf_e2'][:] = psf_e2

        srcVis.extend(tmpCat, False)
        mmatch.add(catalog=tmpCat, dataId=vId)

    # Complete the match, returning a catalog that includes
    # all matched sources with object IDs that can be used to group them.
    matchCat = mmatch.finish()

    # Create a mapping object that allows the matches to be manipulated
    # as a mapping of object ID to catalog of sources.

    # I don't think I can persist a group view, so this may need to be called in a subsequent task
    # allMatches = GroupView.build(matchCat)

    return srcVis, matchCat


def ellipticity_from_cat(cat, slot_shape='slot_Shape'):
    """Calculate the ellipticity of the Shapes in a catalog from the 2nd moments.
    Parameters
    ----------
    cat : `lsst.afw.table.BaseCatalog`
       A catalog with 'slot_Shape' defined and '_xx', '_xy', '_yy'
       entries for the target of 'slot_Shape'.
       E.g., 'slot_shape' defined as 'base_SdssShape'
       And 'base_SdssShape_xx', 'base_SdssShape_xy', 'base_SdssShape_yy' defined.
    slot_shape : str, optional
       Specify what slot shape requested.  Intended use is to get the PSF shape
       estimates by specifying 'slot_shape=slot_PsfShape'
       instead of the default 'slot_shape=slot_Shape'.
    Returns
    -------
    e, e1, e2 : complex, float, float
        Complex ellipticity, real part, imaginary part
    """
    i_xx, i_xy, i_yy = cat.get(slot_shape+'_xx'), cat.get(slot_shape+'_xy'), cat.get(slot_shape+'_yy')
    return ellipticity(i_xx, i_xy, i_yy)


def ellipticity(i_xx, i_xy, i_yy):
    """Calculate ellipticity from second moments.
    Parameters
    ----------
    i_xx : float or `numpy.array`
    i_xy : float or `numpy.array`
    i_yy : float or `numpy.array`
    Returns
    -------
    e, e1, e2 : (float, float, float) or (numpy.array, numpy.array, numpy.array)
        Complex ellipticity, real component, imaginary component
    """
    e = (i_xx - i_yy + 2j*i_xy) / (i_xx + i_yy)
    e1 = np.real(e)
    e2 = np.imag(e)
    return e, e1, e2


def make_matched_photom(vIds, catalogs, photo_calibs):
    # inputs: vIds, catalogs, photo_calibs

    # Match all input bands:
    bands = list(set([f['band'] for f in vIds]))

    # Should probably add an "assert" that requires bands>1...

    empty_cat = catalogs[0].copy()
    empty_cat.clear()

    cat_dict = {}
    mags_dict = {}
    magerrs_dict = {}
    for band in bands:
        cat_dict[band] = empty_cat.copy()
        mags_dict[band] = []
        magerrs_dict[band] = []

    for i in range(len(catalogs)):
        for band in bands:
            if (vIds[i]['band'] in band):
                cat_dict[band].extend(catalogs[i].copy(deep=True))
                mags = photo_calibs[i].instFluxToMagnitude(catalogs[i], 'base_PsfFlux')
                mags_dict[band] = np.append(mags_dict[band], mags[:, 0])
                magerrs_dict[band] = np.append(magerrs_dict[band], mags[:, 1])

    for band in bands:
        cat_tmp = cat_dict[band]
        if cat_tmp:
            if not cat_tmp.isContiguous():
                cat_tmp = cat_tmp.copy(deep=True)
        cat_tmp_final = cat_tmp.asAstropy()
        cat_tmp_final['base_PsfFlux_mag'] = mags_dict[band]
        cat_tmp_final['base_PsfFlux_magErr'] = magerrs_dict[band]
        # Put the bandpass name in the column names:
        for c in cat_tmp_final.colnames:
            if c not in 'id':
                cat_tmp_final[c].name = c+'_'+str(band)
        # Write the new catalog to the dict of catalogs:
        cat_dict[band] = cat_tmp_final

    cat_combined = join(cat_dict[bands[1]], cat_dict[bands[0]], keys='id')
    if len(bands) > 2:
        for i in range(2, len(bands)):
            cat_combined = join(cat_combined, cat_dict[bands[i]], keys='id')

    qual_cuts = (cat_combined['base_ClassificationExtendedness_value_g'] < 0.5) &\
                (cat_combined['base_PixelFlags_flag_saturated_g'] == False) &\
                (cat_combined['base_PixelFlags_flag_cr_g'] == False) &\
                (cat_combined['base_PixelFlags_flag_bad_g'] == False) &\
                (cat_combined['base_PixelFlags_flag_edge_g'] == False) &\
                (cat_combined['base_ClassificationExtendedness_value_r'] < 0.5) &\
                (cat_combined['base_PixelFlags_flag_saturated_r'] == False) &\
                (cat_combined['base_PixelFlags_flag_cr_r'] == False) &\
                (cat_combined['base_PixelFlags_flag_bad_r'] == False) &\
                (cat_combined['base_PixelFlags_flag_edge_r'] == False) &\
                (cat_combined['base_ClassificationExtendedness_value_i'] < 0.5) &\
                (cat_combined['base_PixelFlags_flag_saturated_i'] == False) &\
                (cat_combined['base_PixelFlags_flag_cr_i'] == False) &\
                (cat_combined['base_PixelFlags_flag_bad_i'] == False) &\
                (cat_combined['base_PixelFlags_flag_edge_i'] == False)  # noqa: E712

    # Return the astropy table of matched catalogs:
    return(cat_combined[qual_cuts])


def mergeCatalogs(catalogs,
                  photoCalibs=None, astromCalibs=None,
                  models=['slot_PsfFlux'], applyExternalWcs=False):
    """Merge catalogs and optionally apply photometric and astrometric calibrations.
    """
    
    schema = catalogs[0].schema
    mapper = SchemaMapper(schema)
    mapper.addMinimalSchema(schema)
    aliasMap = schema.getAliasMap()
    for model in models:
        modelName = aliasMap[model] if model in aliasMap.keys() else model
        mapper.addOutputField(Field[float](f'{modelName}_mag',
                                           f'{modelName} magnitude'))
        mapper.addOutputField(Field[float](f'{modelName}_magErr',
                                           f'{modelName} magnitude uncertainty'))
    newSchema = mapper.getOutputSchema()
    newSchema.setAliasMap(schema.getAliasMap())

    size = sum([len(cat) for cat in catalogs])
    catalog = SourceCatalog(newSchema)
    catalog.reserve(size)
        
    for ii in range(0, len(catalogs)):
        cat = catalogs[ii]
        
        # Create temporary catalog. Is this step needed?
        tempCat = SourceCatalog(SourceCatalog(newSchema).table)
        tempCat.extend(cat, mapper=mapper)
        
        if applyExternalWcs and astromCalibs is not None:
            wcs = astromCalibs[ii]
            updateSourceCoords(wcs, tempCat)
        
        if photoCalibs is not None:
            photoCalib = photoCalibs[ii]
            for model in models:
                modelName = aliasMap[model] if model in aliasMap.keys() else model
                photoCalib.instFluxToMagnitude(tempCat, modelName, modelName)
        
        catalog.extend(tempCat)
        
    return catalog