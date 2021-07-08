import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("DetectorMeasurementTaskConfig", "DetectorMeasurementTask",
           "DetectorTableMeasurementTaskConfig", "DetectorTableMeasurementTask")


class DetectorMeasurementTaskConnections(MetricConnections,
                                         dimensions=("instrument", "visit", "detector", "band"),
                                         defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                           "externalPhotoCalibName": "fgcm",
                                                           "wcsName": "calexp.wcs",
                                                           "externalWcsName": "jointcal"}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src"
    )
    skyWcs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="Wcs",
        name="{wcsName}"
    )
    photoCalib = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}"
    )
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
             "id for the catalog id, sorted on id for fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit wcs calibrations computed globally (with no tract information). "
             "These catalogs use the detector id for the catalog id, sorted on id for "
             "fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit photometric calibrations.  These catalogs use the "
             "detector id for the catalog id, sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit photometric calibrations computed globally (with no tract "
             "information).  These catalogs use the detector id for the catalog id, "
             "sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}"
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


class DetectorMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                    pipelineConnections=DetectorMeasurementTaskConnections):
    doApplyExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the external wcs.",
                                            dtype=bool, default=False)
    useGlobalExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the global external wcs.",
                                              dtype=bool, default=False)
    doApplyExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the external photoCalib.",
                                                dtype=bool, default=False)
    useGlobalExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the global external photoCalib.",
                                                  dtype=bool, default=False)


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementTaskConfig
    _DefaultName = "detectorMeasurementTask"

    def run(self, catalog, photoCalib, skyWcs):
        return self.measure.run(catalog, self.config.connections.metric)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        if self.config.doApplyExternalPhotoCalib:
            detector = inputRefs.catalog.dataId['detector']
            if self.config.useGlobalExternalPhotoCalib:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibGlobalCatalog')
            else:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibTractCatalog')
            row = externalPhotoCalibCatalog.find(detector)
            externalPhotoCalib = row.getPhotoCalib()
            inputs['photoCalib'] = externalPhotoCalib
        if self.config.doApplyExternalSkyWcs:
            detector = inputRefs.catalog.dataId['detector']
            if self.config.useGlobalExternalSkyWcs:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsGlobalCatalog')
            else:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsTractCatalog')
            row = externalSkyWcsCatalog.find(detector)
            externalSkyWcs = row.getWcs()
            inputs['skyWcs'] = externalSkyWcs

        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)


class DetectorTableMeasurementTaskConnections(MetricConnections,
                                              dimensions=("instrument", "visit", "detector", "band")):

    catalog = pipeBase.connectionTypes.Input(doc="Source catalog for visit.",
                                             dimensions=("instrument", "visit", "band"),
                                             storageClass="DataFrame",
                                             name="sourceTable_visit",
                                             deferLoad=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-detector measurement.",
                                                  dimensions=("instrument", "visit", "detector", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class DetectorTableMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                         pipelineConnections=DetectorTableMeasurementTaskConnections):
    columns = pexConfig.Field(doc="Columns from sourceTable_visit to load.",
                              dtype=str, default='coord_ra, coord_dec, detector')


class DetectorTableMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorTableMeasurementTaskConfig
    _DefaultName = "detectorTableMeasurementTask"

    def run(self, catalog, dataIds):
        return self.measure.run(catalog, self.config.connections.metric, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        columns = [_.strip() for _ in self.config.columns.split(',')]
        catalog = inputs['catalog'].get(parameters={'columns': columns})
        selection = (catalog['detector'] == butlerQC.quantum.dataId['detector'])
        inputs['catalog'] = catalog[selection]
        inputs['dataIds'] = [butlerQC.registry.expandDataId(inputRefs.catalog.datasetRef.dataId)]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)
