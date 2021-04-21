import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("TractMeasurementTaskConnections", "TractMeasurementTaskConfig",
           "TractMeasurementTask",
           "TractMultiBandMeasurementTaskConnections", "TractMultiBandMeasurementTaskConfig",
           "TractMultiBandMeasurementTask")


class TractMeasurementTaskConnections(MetricConnections,
                                      dimensions=("tract", "skymap",
                                                  "band"),
                                      defaultTemplates={"coaddName": "deepCoadd",
                                                        "photoCalibName": "deepCoadd_calexp.photoCalib",
                                                        "wcsName": "deepCoadd_calexp.wcs"}):

    catalogs = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                              dimensions=("tract", "patch",
                                                          "skymap", "band"),
                                              storageClass="SourceCatalog",
                                              name="deepCoadd_forced_src",
                                              multiple=True)

    photoCalibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                 dimensions=("tract", "patch",
                                                             "skymap", "band"),
                                                 storageClass="PhotoCalib",
                                                 name="{photoCalibName}",
                                                 multiple=True)

    astromCalibs = pipeBase.connectionTypes.Input(doc="WCS for the catalog.",
                                                  dimensions=("tract", "patch",
                                                              "skymap", "band"),
                                                  storageClass="Wcs",
                                                  name="{wcsName}",
                                                  multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-tract measurement.",
                                                  dimensions=("tract", "skymap",
                                                              "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class TractMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                 pipelineConnections=TractMeasurementTaskConnections):
    pass


class TractMeasurementTask(CatalogMeasurementBaseTask):

    ConfigClass = TractMeasurementTaskConfig
    _DefaultName = "tractMeasurementTask"

    def run(self, catalogs, photoCalibs, astromCalibs, dataIds):
        return self.measure.run(self.config.connections.metric, catalogs, photoCalibs, astromCalibs, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['dataIds'] = [butlerQC.registry.expandDataId(cat.dataId) for cat in inputRefs.catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)


class TractMultiBandMeasurementTaskConnections(TractMeasurementTaskConnections,
                                               dimensions=("tract", "skymap"),
                                               defaultTemplates={"coaddName": "deepCoadd", "photoCalibName":
                                                                 "deepCoadd_calexp.photoCalib"}):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "skymap", "patch",
                                                     "band"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src",
                                         multiple=True)

    photoCalibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                 dimensions=("tract", "skymap",
                                                             "patch", "band"),
                                                 storageClass="PhotoCalib",
                                                 name="{photoCalibName}",
                                                 multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-tract measurement.",
                                                  dimensions=("tract", "skymap"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class TractMultiBandMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                          pipelineConnections=TractMultiBandMeasurementTaskConnections):
    pass


class TractMultiBandMeasurementTask(TractMeasurementTask):

    ConfigClass = TractMultiBandMeasurementTaskConfig
    _DefaultName = "tractMultiBandMeasurementTask"
