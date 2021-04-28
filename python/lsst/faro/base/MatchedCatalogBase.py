import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom
import numpy as np

from lsst.faro.utils.matcher import match_catalogs

__all__ = ('MatchedBaseTaskConnections', 'MatchedBaseTaskConfig', 'MatchedBaseTask', 'MatchedTractBaseTask')


class MatchedBaseTaskConnections(pipeBase.PipelineTaskConnections,
                                 dimensions=(),
                                 defaultTemplates={"coaddName": "deep",
                                                   "photoCalibName": "calexp.photoCalib",
                                                   "wcsName": "calexp.wcs",
                                                   "externalPhotoCalibName": "fgcm",
                                                   "externalWcsName": "jointcal"}):
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
    astrom_calibs = pipeBase.connectionTypes.Input(doc="WCS for the catalog.",
                                                   dimensions=("instrument", "visit",
                                                               "detector", "band"),
                                                   storageClass="Wcs",
                                                   name="{wcsName}",
                                                   multiple=True)
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
             "id for the catalog id, sorted on id for fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract", "band"),
        multiple=True
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit wcs calibrations computed globally (with no tract information). "
             "These catalogs use the detector id for the catalog id, sorted on id for "
             "fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "band"),
        multiple=True
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit photometric calibrations.  These catalogs use the "
             "detector id for the catalog id, sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract", "band"),
        multiple=True
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit photometric calibrations computed globally (with no tract "
             "information).  These catalogs use the detector id for the catalog id, "
             "sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "band"),
        multiple=True
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

#    # Hack, this is the only way to get a connection without fixed dims
#    # Inspired by:
#    # https://github.com/lsst/verify/blob/4816a2c/python/lsst/verify/tasks/metadataMetricTask.py#L65-L101
#    def __init__(self, *, config=None):
#        """Customize connection for the astrometric calibrations
#
#        Parameters
#        ----------
#        config : `MatchedBaseTaskConfig`
#            A config for `MatchedBaseTask` or one of its subclasses
#        """
#        super().__init__(config=config)
#        if config and config.wcsDimensions != self.astrom_calibs.dimensions:
#            new_astrom_calibs = pipeBase.connectionTypes.Input(
#                doc=self.astrom_calibs.doc,
#                dimensions=config.wcsDimensions,
#                storageClass=self.astrom_calibs.storageClass,
#                name=self.astrom_calibs.name,
#                multiple=self.astrom_calibs.multiple
#            )
#            self.astrom_calibs = new_astrom_calibs
#            self.allConnections['astrom_calibs'] = self.astrom_calibs


class MatchedBaseTaskConfig(pipeBase.PipelineTaskConfig,
                            pipelineConnections=MatchedBaseTaskConnections):
    match_radius = pexConfig.Field(doc="Match radius in arcseconds.", dtype=float, default=1)
    doApplyExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the external wcs.", 
                                            dtype=bool, default=False)
    useGlobalExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the global external wcs.", 
                                              dtype=bool, default=False)
    doApplyExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the external photoCalib.", 
                                                dtype=bool, default=False)
    useGlobalExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the global external photoCalib.", 
                                                  dtype=bool, default=False)


class MatchedBaseTask(pipeBase.PipelineTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedBaseTask"

    def __init__(self, config: MatchedBaseTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "patch"


    def run(self, source_catalogs, photo_calibs, astrom_calibs, vIds, wcs, box,
            doApplyExternalSkyWcs=False, doApplyExternalPhotoCalib=False):
        self.log.info("Running catalog matching")
        radius = geom.Angle(self.radius, geom.arcseconds)
        srcvis, matched = match_catalogs(source_catalogs, photo_calibs, astrom_calibs, vIds, radius,
                                         logger=self.log)
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
        inputs['doApplyExternalSkyWcs'] = self.config.doApplyExternalSkyWcs
        inputs['doApplyExternalPhotoCalib'] = self.config.doApplyExternalPhotoCalib

        if self.config.doApplyExternalPhotoCalib:
            if self.config.useGlobalExternalPhotoCalib:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibGlobalCatalog')
            else:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibTractCatalog')

            flatPhotoCalibList = np.hstack(externalPhotoCalibCatalog)
            visitPhotoCalibList = np.array([calib['visit'] for calib in flatPhotoCalibList])
            detectorPhotoCalibList = np.array([calib['id'] for calib in flatPhotoCalibList])

        if self.config.doApplyExternalSkyWcs:
            if self.config.useGlobalExternalSkyWcs:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsGlobalCatalog')
            else:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsTractCatalog')

            flatSkyWcsList = np.hstack(externalSkyWcsCatalog)
            visitSkyWcsList = np.array([calib['visit'] for calib in flatSkyWcsList])
            detectorSkyWcsList = np.array([calib['id'] for calib in flatSkyWcsList])

        # import pdb; pdb.set_trace()

        if self.config.doApplyExternalPhotoCalib:
            for i in range(len(inputs['vIds'])):
                dataId = inputs['vIds'][i]
                detector = dataId['detector']
                visit = dataId['visit']
                calib_find = (visitPhotoCalibList == visit) & (detectorPhotoCalibList == detector)
                row = flatPhotoCalibList[calib_find]
                externalPhotoCalib = row[0].getPhotoCalib()
                inputs['photo_calibs'][i] = externalPhotoCalib

        if self.config.doApplyExternalSkyWcs:
            for i in range(len(inputs['vIds'])):
                dataId = inputs['vIds'][i]
                detector = dataId['detector']
                visit = dataId['visit']
                calib_find = (visitSkyWcsList == visit) & (detectorSkyWcsList == detector)
                row = flatSkyWcsList[calib_find]
                externalSkyWcs = row[0].getWcs()
                inputs['astrom_calibs'][i] = externalSkyWcs

        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)


class MatchedTractBaseTask(MatchedBaseTask):

    ConfigClass = MatchedBaseTaskConfig
    _DefaultName = "matchedTractBaseTask"

    def __init__(self, config: MatchedBaseTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.radius = self.config.match_radius
        self.level = "tract"

    def get_box_wcs(self, skymap, oid):
        tract_info = skymap.generateTract(oid['tract'])
        wcs = tract_info.getWcs()
        tract_box = tract_info.getBBox()
        self.log.info(f"Running tract: {oid['tract']}")
        return tract_box, wcs
