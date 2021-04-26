import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("DetectorMeasurementTaskConfig", "DetectorMeasurementTask")


class DetectorMeasurementTaskConnections(MetricConnections,
                                         dimensions=("instrument", "visit", "detector", "band"),
                                         defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                           "wcsName": "calexp.wcs"}):

    catalog = pipeBase.connectionTypes.Input(doc="Source catalog.",
                                             dimensions=("instrument", "visit", "detector", "band"),
                                             storageClass="SourceCatalog",
                                             name="src")

    measurement = pipeBase.connectionTypes.Output(doc="Per-detector measurement.",
                                                  dimensions=("instrument", "visit", "detector", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")

class DetectorMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                    pipelineConnections=DetectorMeasurementTaskConnections):
    pass

class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementTaskConfig
    _DefaultName = "detectorMeasurementTask"

    def run(self, catalog):
        return self.measure.run(catalog, self.config.connections.metric)
