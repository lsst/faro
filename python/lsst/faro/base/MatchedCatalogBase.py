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

import lsst.afw.table as afwTable
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom
from lsst.utils.logging import PeriodicLogger
import numpy as np

from lsst.faro.utils.matcher import matchCatalogs

__all__ = (
    "MatchedBaseConnections",
    "MatchedBaseConfig",
    "MatchedBaseTask",
    "MatchedTractBaseTask",
)


class MatchedBaseConnections(
    pipeBase.PipelineTaskConnections,
    dimensions=(),
    defaultTemplates={"coaddName": "deep"}
):
    sourceCatalogs = pipeBase.connectionTypes.Input(
        doc="Source catalogs to match up.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="initial_stars_footprints_detector",
        multiple=True,
    )
    visitSummary = pipeBase.connectionTypes.Input(
        doc="Exposure catalog with WCS and PhotoCalib this detector+visit combination.",
        dimensions=("instrument", "visit"),
        storageClass="ExposureCatalog",
        name="finalVisitSummary",
        multiple=True,
    )
    skyMap = pipeBase.connectionTypes.Input(
        doc="Input definition of geometry/bbox and projection/wcs for warped exposures",
        name="skyMap",
        storageClass="SkyMap",
        dimensions=("skymap",),
    )


class MatchedBaseConfig(
    pipeBase.PipelineTaskConfig, pipelineConnections=MatchedBaseConnections
):
    match_radius = pexConfig.Field(
        doc="Match radius in arcseconds.", dtype=float, default=1
    )
    snrMin = pexConfig.Field(
        doc="Minimum SNR for a source to be included.",
        dtype=float, default=200
    )
    snrMax = pexConfig.Field(
        doc="Maximum SNR for a source to be included.",
        dtype=float, default=np.inf
    )
    brightMagCut = pexConfig.Field(
        doc="Bright limit of catalog entries to include.", dtype=float, default=10.0
    )
    faintMagCut = pexConfig.Field(
        doc="Faint limit of catalog entries to include.", dtype=float, default=30.0
    )
    selectExtended = pexConfig.Field(
        doc="Whether to select extended sources", dtype=bool, default=False
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
    ):
        self.log.info("Running catalog matching")
        periodicLog = PeriodicLogger(self.log)
        radius = geom.Angle(self.radius, geom.arcseconds)
        if len(sourceCatalogs) < 2:
            self.log.warning("%s valid input catalogs: ", len(sourceCatalogs))
            out_matched = afwTable.SimpleCatalog()
        else:
            srcvis, matched = matchCatalogs(
                sourceCatalogs, photoCalibs, astromCalibs, dataIds, radius,
                self.config, logger=self.log
            )
            self.log.verbose("Finished matching catalogs.")

            # Trim the output to the patch bounding box
            out_matched = type(matched)(matched.schema)
            self.log.info("%s sources in matched catalog.", len(matched))
            for record_index, record in enumerate(matched):
                if box.contains(wcs.skyToPixel(record.getCoord())):
                    out_matched.append(record)
                periodicLog.log("Checked %d records for trimming out of %d.", record_index + 1, len(matched))

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
        self.log.verbose("Inputs obtained from the butler.")
        oid = dict(outputRefs.outputCatalog.dataId.required)
        skymap = inputs["skyMap"]
        del inputs["skyMap"]
        box, wcs = self.get_box_wcs(skymap, oid)
        # Cast to float to handle fractional pixels
        box = geom.Box2D(box)
        inputs["dataIds"] = [el.dataId for el in inputRefs.sourceCatalogs]
        inputs["wcs"] = wcs
        inputs["box"] = box
        visitSummary = inputs.pop("visitSummary")

        flatVisitSummaryList = np.hstack(visitSummary)
        visitSummaryVisitIdList = np.array(
            [calib["visit"] for calib in flatVisitSummaryList]
        )
        visitSummaryDetectorIdList = np.array(
            [calib["id"] for calib in flatVisitSummaryList]
        )

        remove_indices = []
        inputs["photoCalibs"] = [None] * len(inputs["dataIds"])
        inputs["astromCalibs"] = [None] * len(inputs["dataIds"])

        for i in range(len(inputs["dataIds"])):
            dataId = inputs["dataIds"][i]
            detector = dataId["detector"]
            visit = dataId["visit"]
            row_find = (visitSummaryVisitIdList == visit) & (
                visitSummaryDetectorIdList == detector
            )
            if np.sum(row_find) < 1:
                self.log.warning("Detector id %s not found in visit summary "
                                 "for visit %s and will not be used.",
                                 detector, visit)
                inputs["photoCalibs"][i] = None
                inputs["astromCalibs"][i] = None
                remove_indices.append(i)
            else:
                row = flatVisitSummaryList[row_find]
                inputs["photoCalibs"][i] = row[0].getPhotoCalib()
                inputs["astromCalibs"][i] = row[0].getWcs()

        # Remove datasets that didn't have matching external calibs
        remove_indices = np.unique(np.array(remove_indices))
        if len(remove_indices) > 0:
            for ind in sorted(remove_indices, reverse=True):
                del inputs['sourceCatalogs'][ind]
                del inputs['dataIds'][ind]
                del inputs['photoCalibs'][ind]
                del inputs['astromCalibs'][ind]

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
