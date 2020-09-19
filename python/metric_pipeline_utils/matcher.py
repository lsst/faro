from lsst.afw.table import (SchemaMapper, Field,
                            MultiMatch, SimpleRecord,
                            SourceCatalog, updateSourceCoords)

import numpy as np


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
    for oldSrc, photoCalib, wcs, vId in zip(inputs, photoCalibs, astromCalibs, vIds):

        if logger:
            logger.debug(f"{len(oldSrc)} sources in ccd {vId['detector']}  visit {vId['visit']}")

        # create temporary catalog
        tmpCat = SourceCatalog(SourceCatalog(newSchema).table)
        tmpCat.extend(oldSrc, mapper=mapper)

        filtnum = filter_dict[vId['abstract_filter']]
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
