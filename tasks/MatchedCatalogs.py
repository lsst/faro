import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom

from sst_metrics_utils.matcher import match_catalogs

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class MatchedCatalogTaskConnections(pipeBase.PipelineTaskConnections,
                                    dimensions=("tract", "patch", "abstract_filter", "instrument", "skymap"),
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
    skyMap = pipeBase.connectionTypes.Input(
        doc="Input definition of geometry/bbox and projection/wcs for warped exposures",
        name="{coaddName}Coadd_skyMap",
        storageClass="SkyMap",
        dimensions=("skymap",),
    )
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument","abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")


class MatchedCatalogTaskConfig(pipeBase.PipelineTaskConfig,
                               pipelineConnections=MatchedCatalogTaskConnections):
    match_radius = pexConfig.Field(doc="Match radius in arcseconds.", dtype=float, default=1)


class MatchedCatalogTask(pipeBase.PipelineTask):

    ConfigClass = MatchedCatalogTaskConfig
    _DefaultName = "matchedCatalogTask"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius

    def run(self, source_catalogs, photo_calibs, vIds, wcs, box):
        self.log.info(f"Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = match_catalogs(source_catalogs, photo_calibs, vIds, radius)
        # Trim the output to the patch bounding box
        out_matched = type(matched)(matched.schema)
        for record in matched:
            if box.contains(wcs.skyToPixel(record.getCoord())):
                out_matched.append(record)
        return pipeBase.Struct(outputCatalog=out_matched)

    def runQuantum(self, butlerQC,
                   inputRefs,
                   outputRefs):
        inputs = butlerQC.get(inputRefs)
        oid = outputRefs.__dict__['outputCatalog'].dataId.byName()
        skymap = inputs['skyMap']
        del inputs['skyMap']
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        patch_info = tract_info.getPatchInfo(oid['patch'])
        patch_box = patch_info.getInnerBBox()
        # Cast to float to handle fractional pixels
        patch_box = geom.Box2D(patch_box)
        inputs['vIds'] = [el.dataId.byName() for el in inputRefs.__dict__['source_catalogs']]
        inputs['wcs'] = wcs
        inputs['box'] = patch_box
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)