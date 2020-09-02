import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom

from metric_pipeline_utils.matcher import match_catalogs


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
# Should not be used alone, subclasses should define dimensions and output
class MatchedBaseTaskConnections(pipeBase.PipelineTaskConnections,
                                 dimensions=(),
                                 defaultTemplates={"coaddName": "deep"}):
    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs to match up.",
                                                     dimensions=("instrument", "visit",
                                                                 "detector", "abstract_filter"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)
    photo_calibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                  dimensions=("instrument", "visit",
                                                              "detector", "abstract_filter"),
                                                  storageClass="PhotoCalib",
                                                  name="calexp.photoCalib",
                                                  multiple=True)
    skyMap = pipeBase.connectionTypes.Input(
        doc="Input definition of geometry/bbox and projection/wcs for warped exposures",
        name="{coaddName}Coadd_skyMap",
        storageClass="SkyMap",
        dimensions=("skymap",),
    )


class MatchedBaseTaskConfig(pipeBase.PipelineTaskConfig,
                            pipelineConnections=MatchedBaseTaskConnections):
    match_radius = pexConfig.Field(doc="Match radius in arcseconds.", dtype=float, default=1)


class MatchedBaseTask(pipeBase.PipelineTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedBaseTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius

    def run(self, source_catalogs, photo_calibs, vIds, wcs, box):
        self.log.info(f"Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = match_catalogs(source_catalogs, photo_calibs, vIds, radius, logger=self.log)
        # Trim the output to the patch bounding box
        out_matched = type(matched)(matched.schema)
        self.log.info(f"{len(matched)} sources in matched catalog.")
        for record in matched:
            if box.contains(wcs.skyToPixel(record.getCoord())):
                out_matched.append(record)
        self.log.info(f"{len(out_matched)} sources when trimmed to patch boundaries.")
        return pipeBase.Struct(outputCatalog=out_matched)

    def runQuantum(self, butlerQC,
                   inputRefs,
                   outputRefs):
        inputs = butlerQC.get(inputRefs)
        oid = outputRefs.outputCatalog.dataId.byName()
        skymap = inputs['skyMap']
        del inputs['skyMap']
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        patch_info = tract_info.getPatchInfo(oid['patch'])
        patch_box = patch_info.getInnerBBox()
        self.log.info(f"Running tract: {oid['tract']} and patch: {oid['patch']}")
        # Cast to float to handle fractional pixels
        patch_box = geom.Box2D(patch_box)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        inputs['wcs'] = wcs
        inputs['box'] = patch_box
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)


class MatchedTractBaseTask(pipeBase.PipelineTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedTractBaseTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius

    def run(self, source_catalogs, photo_calibs, vIds, wcs, box):
        self.log.info(f"Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = match_catalogs(source_catalogs, photo_calibs, vIds, radius, logger=self.log)
        # Trim the output to the tract bounding box
        out_matched = type(matched)(matched.schema)
        self.log.info(f"{len(matched)} sources in matched catalog.")
        for record in matched:
            if box.contains(wcs.skyToPixel(record.getCoord())):
                out_matched.append(record)
        self.log.info(f"{len(out_matched)} sources when trimmed to tract boundaries.")
        return pipeBase.Struct(outputCatalog=out_matched)

    def runQuantum(self, butlerQC,
                   inputRefs,
                   outputRefs):
        inputs = butlerQC.get(inputRefs)
        oid = outputRefs.outputCatalog.dataId.byName()
        skymap = inputs['skyMap']
        del inputs['skyMap']
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        tract_box = tract_info.getBBox()
        # patch_info = tract_info.getPatchInfo(oid['patch'])
        # patch_box = patch_info.getInnerBBox()
        self.log.info(f"Running tract: {oid['tract']}")
        # Cast to float to handle fractional pixels
        tract_box = geom.Box2D(tract_box)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        inputs['wcs'] = wcs
        inputs['box'] = tract_box
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)
