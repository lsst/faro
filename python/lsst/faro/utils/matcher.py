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

from lsst.afw.geom import SkyWcs
from lsst.afw.image import PhotoCalib
from lsst.afw.table import (
    SchemaMapper,
    Field,
    MultiMatch,
    SimpleRecord,
    SourceCatalog,
    updateSourceCoords,
)
from lsst.faro.utils.calibrated_catalog import CalibratedCatalog

import numpy as np
from astropy.table import join, Table
from typing import Dict, List

__all__ = (
    "matchCatalogs",
    "ellipticityFromCat",
    "ellipticity",
    "makeMatchedPhotom",
    "mergeCatalogs",
)


def matchCatalogs(
        inputs: List[SourceCatalog],
        photoCalibs: List[PhotoCalib],
        astromCalibs: List[SkyWcs],
        dataIds,
        matchRadius: float,
        logger=None,
):
    schema = inputs[0].schema
    mapper = SchemaMapper(schema)
    mapper.addMinimalSchema(schema)
    mapper.addOutputField(Field[float]("base_PsfFlux_snr", "PSF flux SNR"))
    mapper.addOutputField(Field[float]("base_PsfFlux_mag", "PSF magnitude"))
    mapper.addOutputField(
        Field[float]("base_PsfFlux_magErr", "PSF magnitude uncertainty")
    )
    # Needed because addOutputField(... 'slot_ModelFlux_mag') will add a field with that literal name
    aliasMap = schema.getAliasMap()
    # Possibly not needed since base_GaussianFlux is the default, but this ought to be safe
    modelName = (
        aliasMap["slot_ModelFlux"]
        if "slot_ModelFlux" in aliasMap.keys()
        else "base_GaussianFlux"
    )
    mapper.addOutputField(Field[float](f"{modelName}_mag", "Model magnitude"))
    mapper.addOutputField(
        Field[float](f"{modelName}_magErr", "Model magnitude uncertainty")
    )
    mapper.addOutputField(Field[float](f"{modelName}_snr", "Model flux snr"))
    mapper.addOutputField(Field[float]("e1", "Source Ellipticity 1"))
    mapper.addOutputField(Field[float]("e2", "Source Ellipticity 1"))
    mapper.addOutputField(Field[float]("psf_e1", "PSF Ellipticity 1"))
    mapper.addOutputField(Field[float]("psf_e2", "PSF Ellipticity 1"))
    mapper.addOutputField(Field[np.int32]("filt", "filter code"))
    newSchema = mapper.getOutputSchema()
    newSchema.setAliasMap(schema.getAliasMap())

    # Create an object that matches multiple catalogs with same schema
    mmatch = MultiMatch(
        newSchema,
        dataIdFormat={"visit": np.int32, "detector": np.int32},
        radius=matchRadius,
        RecordClass=SimpleRecord,
    )

    # create the new extended source catalog
    srcVis = SourceCatalog(newSchema)

    filter_dict = {
        "u": 1,
        "g": 2,
        "r": 3,
        "i": 4,
        "z": 5,
        "y": 6,
        "HSC-U": 1,
        "HSC-G": 2,
        "HSC-R": 3,
        "HSC-I": 4,
        "HSC-Z": 5,
        "HSC-Y": 6,
    }

    # Sort by visit, detector, then filter
    vislist = [v["visit"] for v in dataIds]
    ccdlist = [v["detector"] for v in dataIds]
    filtlist = [v["band"] for v in dataIds]
    tab_vids = Table([vislist, ccdlist, filtlist], names=["vis", "ccd", "filt"])
    sortinds = np.argsort(tab_vids, order=("vis", "ccd", "filt"))

    for ind in sortinds:
        oldSrc = inputs[ind]
        photoCalib = photoCalibs[ind]
        wcs = astromCalibs[ind]
        dataId = dataIds[ind]

        if logger:
            logger.debug(
                "%d sources in ccd %s visit %s",
                len(oldSrc),
                dataId["detector"],
                dataId["visit"],
            )

        # create temporary catalog
        tmpCat = SourceCatalog(SourceCatalog(newSchema).table)
        tmpCat.extend(oldSrc, mapper=mapper)

        filtnum = filter_dict[dataId["band"]]
        tmpCat["filt"] = np.repeat(filtnum, len(oldSrc))

        tmpCat["base_PsfFlux_snr"][:] = (
            tmpCat["base_PsfFlux_instFlux"] / tmpCat["base_PsfFlux_instFluxErr"]
        )

        updateSourceCoords(wcs, tmpCat)

        photoCalib.instFluxToMagnitude(tmpCat, "base_PsfFlux", "base_PsfFlux")
        tmpCat["slot_ModelFlux_snr"][:] = (
            tmpCat["slot_ModelFlux_instFlux"] / tmpCat["slot_ModelFlux_instFluxErr"]
        )
        photoCalib.instFluxToMagnitude(tmpCat, "slot_ModelFlux", "slot_ModelFlux")

        _, psf_e1, psf_e2 = ellipticityFromCat(oldSrc, slot_shape="slot_PsfShape")
        _, star_e1, star_e2 = ellipticityFromCat(oldSrc, slot_shape="slot_Shape")
        tmpCat["e1"][:] = star_e1
        tmpCat["e2"][:] = star_e2
        tmpCat["psf_e1"][:] = psf_e1
        tmpCat["psf_e2"][:] = psf_e2

        srcVis.extend(tmpCat, False)
        mmatch.add(catalog=tmpCat, dataId=dataId)

    # Complete the match, returning a catalog that includes
    # all matched sources with object IDs that can be used to group them.
    matchCat = mmatch.finish()

    # Create a mapping object that allows the matches to be manipulated
    # as a mapping of object ID to catalog of sources.

    # I don't think I can persist a group view, so this may need to be called in a subsequent task
    # allMatches = GroupView.build(matchCat)

    return srcVis, matchCat


def ellipticityFromCat(cat, slot_shape="slot_Shape"):
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
    i_xx, i_xy, i_yy = (
        cat.get(slot_shape + "_xx"),
        cat.get(slot_shape + "_xy"),
        cat.get(slot_shape + "_yy"),
    )
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
    e = (i_xx - i_yy + 2j * i_xy) / (i_xx + i_yy)
    e1 = np.real(e)
    e2 = np.imag(e)
    return e, e1, e2


def makeMatchedPhotom(data: Dict[str, List[CalibratedCatalog]], logger=None):
    """ Merge catalogs in multiple bands into a single shared catalog.
    """

    cat_all = None

    for band, cat_list in data.items():
        cat_tmp = []
        calibs_photo = []
        for cat_calib in cat_list:
            cat_tmp_i = cat_calib.catalog
            qual_cuts = (
                (cat_tmp_i["base_ClassificationExtendedness_value"] < 0.5)
                & ~cat_tmp_i["base_PixelFlags_flag_saturated"]
                & ~cat_tmp_i["base_PixelFlags_flag_cr"]
                & ~cat_tmp_i["base_PixelFlags_flag_bad"]
                & ~cat_tmp_i["base_PixelFlags_flag_edge"]
            )
            cat_tmp.append(cat_tmp_i[qual_cuts])
            calibs_photo.append(cat_calib.photoCalib)

        if logger:
            logger.debug("Merging %d catalogs for band %s.", len(cat_tmp), band)
        cat_tmp = mergeCatalogs(cat_tmp, calibs_photo, models=['base_PsfFlux'],
                                logger=logger)
        if cat_tmp:
            if not cat_tmp.isContiguous():
                if logger:
                    logger.debug("Deep copying the %s band catalog to make it "
                                 "contiguous.", band)
                cat_tmp = cat_tmp.copy(deep=True)

        cat_tmp = cat_tmp.asAstropy()

        # Put the bandpass name in the column names:
        for c in cat_tmp.colnames:
            if c != "id":
                cat_tmp[c].name = f"{c}_{band}"

        if cat_all:
            if logger:
                logger.debug("Joining the %s band catalog with the main "
                             "catalog.", band)
            cat_all = join(cat_all, cat_tmp, keys="id")
        else:
            cat_all = cat_tmp

    # Return the astropy table of matched catalogs:
    return cat_all


def mergeCatalogs(
    catalogs,
    photoCalibs=None,
    astromCalibs=None,
    models=["slot_PsfFlux"],
    applyExternalWcs=False,
    logger=None,
):
    """Merge catalogs and optionally apply photometric and astrometric calibrations.
    """

    schema = catalogs[0].schema
    mapper = SchemaMapper(schema)
    mapper.addMinimalSchema(schema)
    aliasMap = schema.getAliasMap()
    for model in models:
        modelName = aliasMap[model] if model in aliasMap.keys() else model
        mapper.addOutputField(
            Field[float](f"{modelName}_mag", f"{modelName} magnitude")
        )
        mapper.addOutputField(
            Field[float](f"{modelName}_magErr", f"{modelName} magnitude uncertainty")
        )
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
            if photoCalib is not None:
                for model in models:
                    modelName = aliasMap[model] if model in aliasMap.keys() else model
                    photoCalib.instFluxToMagnitude(tempCat, modelName, modelName)

        catalog.extend(tempCat)

        if logger:
            logger.verbose("Merged %d catalog(s) out of %d." % (ii + 1, len(cat)))

    return catalog
