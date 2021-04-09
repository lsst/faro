import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
from lsst.afw.table import SourceCatalog

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask
from lsst.faro.utils.matcher import merge_catalogs

__all__ = ("VisitMeasurementTaskConfig", "VisitMeasurementTask")


class VisitMeasurementTaskConnections(MetricConnections,
                                      dimensions=("instrument", "visit", "band"),
                                      defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                        "wcsName": "calexp.wcs"}):

    catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs.",
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

    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class VisitMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                 pipelineConnections=VisitMeasurementTaskConnections):
    pass


class VisitMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitMeasurementTaskConfig
    _DefaultName = "visitMeasurementTask"

    def run(self, catalogs, photo_calibs, astrom_calibs, data_ids):
        return self.measure.run(self.config.connections.metric, catalogs, photo_calibs, astrom_calibs, data_ids)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['data_ids'] = [butlerQC.registry.expandDataId(c.dataId) for c in inputRefs.catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
