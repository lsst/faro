import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("DetectorMeasurementTaskConfig", "DetectorMeasurementTask")


class DetectorMeasurementTaskConnections(MetricConnections,
                                         dimensions=("instrument", "visit", "detector", "band"),
                                         defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                           "wcsName": "calexp.wcs",
                                                           "skyWcsName": "jointcal",
                                                           "extPhotoCalibName": "fgcm"}):

    catalog = pipeBase.connectionTypes.Input(doc="Source catalog.",
                                             dimensions=("instrument", "visit", "detector", "band"),
                                             storageClass="SourceCatalog",
                                             name="src")

    measurement = pipeBase.connectionTypes.Output(doc="Per-detector measurement.",
                                                  dimensions=("instrument", "visit", "detector", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
             "id for the catalog id, sorted on id for fast lookup."),
        name="{skyWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit wcs calibrations computed globally (with no tract information). "
             "These catalogs use the detector id for the catalog id, sorted on id for "
             "fast lookup."),
        name="{skyWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit photometric calibrations.  These catalogs use the "
             "detector id for the catalog id, sorted on id for fast lookup."),
        name="{extPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit photometric calibrations computed globally (with no tract "
             "information).  These catalogs use the detector id for the catalog id, "
             "sorted on id for fast lookup."),
        name="{extPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
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
    useGlobalExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the global external WCS.", dtype=bool, default=False)
    doApplyExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the external WCS.", dtype=bool, default=False)
    useGlobalExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the global external photoCalib.", dtype=bool, default=False)
    doApplyExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the external photoCalib.", dtype=bool, default=False)


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementTaskConfig
    _DefaultName = "detectorMeasurementTask"

    def run(self, catalog, externalSkyWcsCatalog=None, externalPhotoCalibCatalog=None):
        return self.measure.run(catalog, self.config.connections.metric)
