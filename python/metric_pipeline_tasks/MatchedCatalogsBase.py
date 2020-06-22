import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom

from sst_metrics_utils.matcher import match_catalogs

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires

# Should not be used alone, subclasses should define dimensions and output
class MatchedBaseTaskConnections(pipeBase.PipelineTaskConnections,
                                 dimensions=(),
                                 defaultTemplates={"coaddName": "deep"}):
    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs to match up.",
                                                     dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)
    photo_calibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                     dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                     storageClass="PhotoCalib",
                                                     name="calexp.photoCalib",
                                                     multiple=True)
    wcslist = pipeBase.connectionTypes.Input(doc="Wcs object.",
                                                     dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                     storageClass="Wcs",
                                                     name="calexp.wcs",
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

    def run(self, source_catalogs, photo_calibs, vIds, wcs, box, wcslist):
        self.log.info(f"Running catalog matching")

        scats = []
        ids = []
        pcalibs = []
        for c, i, p, w in zip(source_catalogs, vIds, photo_calibs, wcslist):
            b = geom.Box2I(geom.Point2I(0, 0), geom.Extent2I(2048, 4176))
            pmin = b.getMin()
            pmax = b.getMax()
            smin = w.pixelToSky(geom.Point2D(pmin))
            smax = w.pixelToSky(geom.Point2D(pmax))
            tmin = wcs.skyToPixel(smin)
            tmax = wcs.skyToPixel(smax)
            pbox = geom.Box2D(tmin, tmax)
            if box.overlaps(pbox):
                scats.append(c)
                ids.append(i)
                pcalibs.append(p)

        radius = geom.Angle(self.radius, geom.arcseconds)
        if len(scats) > 0:
            srcvis, matched = match_catalogs(scats, pcalibs, ids, radius, logger=self.log)
            # Trim the output to the patch bounding box
            out_matched = type(matched)(matched.schema)
            self.log.info(f"{len(matched)} sources in matched catalog.")
            for record in matched:
                if box.contains(wcs.skyToPixel(record.getCoord())):
                    out_matched.append(record)
            self.log.info(f"{len(out_matched)} sources when trimmed to patch boundaries.")
        else:
            srcvis, matched = match_catalogs([source_catalogs[0],], [photo_calibs[0],], [vIds[0],], radius, logger=self.log)
            out_matched = type(matched)(matched.schema)
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
