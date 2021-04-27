import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("DetectorMeasurementTaskConfig", "DetectorMeasurementTask")


class DetectorMeasurementTaskConnections(MetricConnections,
                                         dimensions=("instrument", "visit", "detector", "band"),
                                         defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                           "externalPhotoCalibName": "fgcm",
                                                           "wcsName": "calexp.wcs"}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src"
    )
    photoCalib = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}"
    )
    #externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
    #    doc=("Per-tract, per-visit photometric calibrations.  These catalogs use the "
    #         "detector id for the catalog id, sorted on id for fast lookup."),
    #    name="{externalPhotoCalibName}PhotoCalibCatalog",
    #    storageClass="ExposureCatalog",
    #    dimensions=("instrument", "visit", "tract", "band"),
    #)
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit photometric calibrations computed globally (with no tract "
             "information).  These catalogs use the detector id for the catalog id, "
             "sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "band"),
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}"
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.doApplyExternalPhotoCalib:
            self.inputs.remove("photoCalib")
        else:
            self.inputs.remove("externalPhotoCalibGlobalCatalog")


class DetectorMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                    pipelineConnections=DetectorMeasurementTaskConnections):
    doApplyExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the external photoCalib.", 
                                                dtype=bool, default=False)


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementTaskConfig
    _DefaultName = "detectorMeasurementTask"

    def run(self, catalog, photoCalib):
        return self.measure.run(catalog, self.config.connections.metric)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        if self.config.doApplyExternalPhotoCalib:
            detector = inputRefs.catalog.dataId['detector']
            externalPhotoCalibGlobalCatalog = inputs.pop('externalPhotoCalibGlobalCatalog')
            row = externalPhotoCalibGlobalCatalog.find(detector)
            externalPhotoCalib = row.getPhotoCalib()
            inputs['photoCalib'] = externalPhotoCalib
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
        
