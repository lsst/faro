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

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom
import numpy as np

from lsst.faro.utils.matcher import matchCatalogs
from lsst.faro.base.MatchedCatalogBase import (
    MatchedBaseConnections,
    MatchedBaseConfig,
    MatchedBaseTask,
    MatchedTractBaseTask
)

__all__ = (
    "MatchedBaseTableConnections",
    "MatchedBaseTableConfig",
    "MatchedBaseTableTask",
    "MatchedTractBaseTableTask",
)


class MatchedBaseTableConnections(
    MatchedBaseConnections,
    dimensions=(),
    defaultTemplates={
        "coaddName": "deep",
        "photoCalibName": "calexp.photoCalib",
        "wcsName": "calexp.wcs",
        "externalPhotoCalibName": "fgcm",
        "externalWcsName": "jointcal",
    },
):
    sourceCatalogs = pipeBase.connectionTypes.Input(
        doc="Source catalogs, in parquet format, to match up.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="DataFrame",
        name="sourceTable_visit",
        multiple=True,
        deferLoad=True,
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.doApplyExternalSkyWcs:
            if config.useGlobalExternalSkyWcs:
                self.inputs.remove("externalSkyWcsTractCatalog")
            else:
                self.inputs.remove("externalSkyWcsGlobalCatalog")
        else:
            self.inputs.remove("externalSkyWcsTractCatalog")
            self.inputs.remove("externalSkyWcsGlobalCatalog")
        if config.doApplyExternalPhotoCalib:
            if config.useGlobalExternalPhotoCalib:
                self.inputs.remove("externalPhotoCalibTractCatalog")
            else:
                self.inputs.remove("externalPhotoCalibGlobalCatalog")
        else:
            self.inputs.remove("externalPhotoCalibTractCatalog")
            self.inputs.remove("externalPhotoCalibGlobalCatalog")


'''
class MatchedBaseTableConnections(
    pipeBase.PipelineTaskConnections,
    dimensions=(),
    defaultTemplates={
        "coaddName": "deep",
        "photoCalibName": "calexp.photoCalib",
        "wcsName": "calexp.wcs",
        "externalPhotoCalibName": "fgcm",
        "externalWcsName": "jointcal",
    },
):
    sourceCatalogs = pipeBase.connectionTypes.Input(
        doc="Source catalogs, in parquet format, to match up.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="DataFrame",
        name="sourceTable_visit",
        multiple=True,
        deferLoad=True,
    )
    photoCalibs = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}",
        multiple=True,
    )
    astromCalibs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="Wcs",
        name="{wcsName}",
        multiple=True,
    )
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
            "id for the catalog id, sorted on id for fast lookup."
        ),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract", "band"),
        multiple=True,
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-visit wcs calibrations computed globally (with no tract information). "
            "These catalogs use the detector id for the catalog id, sorted on id for "
            "fast lookup."
        ),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "band"),
        multiple=True,
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-tract, per-visit photometric calibrations.  These catalogs use the "
            "detector id for the catalog id, sorted on id for fast lookup."
        ),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract", "band"),
        multiple=True,
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-visit photometric calibrations computed globally (with no tract "
            "information).  These catalogs use the detector id for the catalog id, "
            "sorted on id for fast lookup."
        ),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "band"),
        multiple=True,
    )
    skyMap = pipeBase.connectionTypes.Input(
        doc="Input definition of geometry/bbox and projection/wcs for warped exposures",
        name="skyMap",
        storageClass="SkyMap",
        dimensions=("skymap",),
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.doApplyExternalSkyWcs:
            if config.useGlobalExternalSkyWcs:
                self.inputs.remove("externalSkyWcsTractCatalog")
            else:
                self.inputs.remove("externalSkyWcsGlobalCatalog")
        else:
            self.inputs.remove("externalSkyWcsTractCatalog")
            self.inputs.remove("externalSkyWcsGlobalCatalog")
        if config.doApplyExternalPhotoCalib:
            if config.useGlobalExternalPhotoCalib:
                self.inputs.remove("externalPhotoCalibTractCatalog")
            else:
                self.inputs.remove("externalPhotoCalibGlobalCatalog")
        else:
            self.inputs.remove("externalPhotoCalibTractCatalog")
            self.inputs.remove("externalPhotoCalibGlobalCatalog")
'''


class MatchedBaseConfig(
    pipeBase.PipelineTaskConfig, pipelineConnections=MatchedBaseConnections
):
    match_radius = pexConfig.Field(
        doc="Match radius in arcseconds.", dtype=float, default=1
    )
    doApplyExternalSkyWcs = pexConfig.Field(
        doc="Whether or not to use the external wcs.", dtype=bool, default=False
    )
    useGlobalExternalSkyWcs = pexConfig.Field(
        doc="Whether or not to use the global external wcs.", dtype=bool, default=False
    )
    doApplyExternalPhotoCalib = pexConfig.Field(
        doc="Whether or not to use the external photoCalib.", dtype=bool, default=False
    )
    useGlobalExternalPhotoCalib = pexConfig.Field(
        doc="Whether or not to use the global external photoCalib.",
        dtype=bool,
        default=False,
    )


class MatchedBaseTask(pipeBase.PipelineTask):

    ConfigClass = MatchedBaseConfig
    _DefaultName = "matchedBaseTask"

    def __init__(self, config: MatchedBaseConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "patch"

    def run(
        self,
        sourceCatalogs,
        photoCalibs,
        astromCalibs,
        dataIds,
        wcs,
        box,
        doApplyExternalSkyWcs=False,
        doApplyExternalPhotoCalib=False,
    ):
        self.log.info("Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = matchCatalogs(
            sourceCatalogs, photoCalibs, astromCalibs, dataIds, radius, logger=self.log
        )
        # Trim the output to the patch bounding box
        out_matched = type(matched)(matched.schema)
        self.log.info("%s sources in matched catalog.", len(matched))
        for record in matched:
            if box.contains(wcs.skyToPixel(record.getCoord())):
                out_matched.append(record)
        self.log.info(
            "%s sources when trimmed to %s boundaries.", len(out_matched), self.level
        )
        return pipeBase.Struct(outputCatalog=out_matched)

    def get_box_wcs(self, skymap, oid):
        tract_info = skymap.generateTract(oid["tract"])
        wcs = tract_info.getWcs()
        patch_info = tract_info.getPatchInfo(oid["patch"])
        patch_box = patch_info.getInnerBBox()
        self.log.info("Running tract: %s and patch: %s", oid["tract"], oid["patch"])
        return patch_box, wcs

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        oid = outputRefs.outputCatalog.dataId.byName()
        skymap = inputs["skyMap"]
        del inputs["skyMap"]
        box, wcs = self.get_box_wcs(skymap, oid)
        # Cast to float to handle fractional pixels
        box = geom.Box2D(box)
        inputs["dataIds"] = [
            butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.sourceCatalogs
        ]
        inputs["wcs"] = wcs
        inputs["box"] = box
        inputs["doApplyExternalSkyWcs"] = self.config.doApplyExternalSkyWcs
        inputs["doApplyExternalPhotoCalib"] = self.config.doApplyExternalPhotoCalib

        if self.config.doApplyExternalPhotoCalib:
            if self.config.useGlobalExternalPhotoCalib:
                externalPhotoCalibCatalog = inputs.pop(
                    "externalPhotoCalibGlobalCatalog"
                )
            else:
                externalPhotoCalibCatalog = inputs.pop("externalPhotoCalibTractCatalog")

            flatPhotoCalibList = np.hstack(externalPhotoCalibCatalog)
            visitPhotoCalibList = np.array(
                [calib["visit"] for calib in flatPhotoCalibList]
            )
            detectorPhotoCalibList = np.array(
                [calib["id"] for calib in flatPhotoCalibList]
            )

        if self.config.doApplyExternalSkyWcs:
            if self.config.useGlobalExternalSkyWcs:
                externalSkyWcsCatalog = inputs.pop("externalSkyWcsGlobalCatalog")
            else:
                externalSkyWcsCatalog = inputs.pop("externalSkyWcsTractCatalog")

            flatSkyWcsList = np.hstack(externalSkyWcsCatalog)
            visitSkyWcsList = np.array([calib["visit"] for calib in flatSkyWcsList])
            detectorSkyWcsList = np.array([calib["id"] for calib in flatSkyWcsList])

        if self.config.doApplyExternalPhotoCalib:
            for i in range(len(inputs["dataIds"])):
                dataId = inputs["dataIds"][i]
                detector = dataId["detector"]
                visit = dataId["visit"]
                calib_find = (visitPhotoCalibList == visit) & (
                    detectorPhotoCalibList == detector
                )
                row = flatPhotoCalibList[calib_find]
                externalPhotoCalib = row[0].getPhotoCalib()
                inputs["photoCalibs"][i] = externalPhotoCalib

        if self.config.doApplyExternalSkyWcs:
            for i in range(len(inputs["dataIds"])):
                dataId = inputs["dataIds"][i]
                detector = dataId["detector"]
                visit = dataId["visit"]
                calib_find = (visitSkyWcsList == visit) & (
                    detectorSkyWcsList == detector
                )
                row = flatSkyWcsList[calib_find]
                externalSkyWcs = row[0].getWcs()
                inputs["astromCalibs"][i] = externalSkyWcs

        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)


class MatchedTractBaseTask(MatchedBaseTask):

    ConfigClass = MatchedBaseConfig
    _DefaultName = "matchedTractBaseTask"

    def __init__(self, config: MatchedBaseConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "tract"

    def get_box_wcs(self, skymap, oid):
        tract_info = skymap.generateTract(oid["tract"])
        wcs = tract_info.getWcs()
        tract_box = tract_info.getBBox()
        self.log.info("Running tract: %s", oid["tract"])
        return tract_box, wcs