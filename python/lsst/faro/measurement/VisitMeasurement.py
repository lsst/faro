import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

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

    photoCalibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                 dimensions=("instrument", "visit",
                                                             "detector", "band"),
                                                 storageClass="PhotoCalib",
                                                 name="{photoCalibName}",
                                                 multiple=True)

    astromCalibs = pipeBase.connectionTypes.Input(doc="WCS for the catalog.",
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

    def run(self, catalogs, photoCalibs, astromCalibs, dataIds):
        return self.measure.run(self.config.connections.metric, catalogs, photoCalibs, astromCalibs, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['dataIds'] = [butlerQC.registry.expandDataId(c.dataId) for c in inputRefs.catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
