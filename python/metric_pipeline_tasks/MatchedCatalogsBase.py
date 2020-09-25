import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom

from metric_pipeline_utils.matcher import match_catalogs


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
# Should not be used alone, subclasses should define dimensions and output
class MatchedBaseTaskConnections(pipeBase.PipelineTaskConnections,
                                 dimensions=(),
                                 defaultTemplates={"coaddName": "deep", "photoCalibName":
                                                   "calexp.photoCalib"}):
    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs to match up.",
                                                     dimensions=("instrument", "visit",
                                                                 "detector", "band"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)
    photo_calibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                  dimensions=("instrument", "visit",
                                                              "detector", "band"),
                                                  storageClass="PhotoCalib",
                                                  name="{photoCalibName}",
                                                  multiple=True)
    astrom_calibs = pipeBase.connectionTypes.PrerequisiteInput(doc="WCS for the catalog.",
                                                               dimensions=("instrument", "visit",
                                                                           "skymap", "tract",
                                                                           "detector", "band"),
                                                               storageClass="Wcs",
                                                               name="jointcal_wcs",
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
    apply_external_wcs = pexConfig.Field(doc="Apply correction to coordinates with e.g. a jointcal WCS.",
                                         dtype=bool, default=False)


class MatchedBaseTask(pipeBase.PipelineTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedBaseTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "patch"

    def run(self, source_catalogs, photo_calibs, astrom_calibs, vIds, wcs, box, apply_external_wcs):
        self.log.info(f"Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = match_catalogs(source_catalogs, photo_calibs, astrom_calibs, vIds, radius,
                                         apply_external_wcs, logger=self.log)
        # Trim the output to the patch bounding box
        out_matched = type(matched)(matched.schema)
        self.log.info(f"{len(matched)} sources in matched catalog.")
        for record in matched:
            if box.contains(wcs.skyToPixel(record.getCoord())):
                out_matched.append(record)
        self.log.info(f"{len(out_matched)} sources when trimmed to {self.level} boundaries.")
        return pipeBase.Struct(outputCatalog=out_matched)

    def get_box_wcs(self, skymap, oid):
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        patch_info = tract_info.getPatchInfo(oid['patch'])
        patch_box = patch_info.getInnerBBox()
        self.log.info(f"Running tract: {oid['tract']} and patch: {oid['patch']}")
        return patch_box, wcs

    def runQuantum(self, butlerQC,
                   inputRefs,
                   outputRefs):
        inputs = butlerQC.get(inputRefs)
        oid = outputRefs.outputCatalog.dataId.byName()
        skymap = inputs['skyMap']
        del inputs['skyMap']
        box, wcs = self.get_box_wcs(skymap, oid)
        # Cast to float to handle fractional pixels
        box = geom.Box2D(box)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        inputs['wcs'] = wcs
        inputs['box'] = box
        inputs['apply_external_wcs'] = self.config.apply_external_wcs
        if inputs['apply_external_wcs'] and not inputs['astrom_calibs']:
            self.log.warn('Task configured to apply an external WCS, but no external WCS datasets found.')
        if not inputs['astrom_calibs']:  # Fill with None if jointcal wcs doesn't exist
            inputs['astrom_calibs'] = [None for el in inputs['photo_calibs']]
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)


class MatchedTractBaseTask(MatchedBaseTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedTractBaseTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "tract"

    def get_box_wcs(self, skymap, oid):
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        tract_box = tract_info.getBBox()
        self.log.info(f"Running tract: {oid['tract']}")
        return tract_box, wcs
