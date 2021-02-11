import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from lsst.faro.base.CatalogMeasureBase import CatalogMeasureBaseTaskConfig, CatalogMeasureBaseTask


class TractMeasTaskConnections(MetricConnections,
                               dimensions=("tract", "skymap",
                                           "band"),
                               defaultTemplates={"coaddName": "deepCoadd",
                                                 "photoCalibName": "deepCoadd_calexp.photoCalib"}):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "skymap",
                                                     "band"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src")

    photo_calibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                  dimensions=("tract", "skymap",
                                                              "band"),
                                                  storageClass="PhotoCalib",
                                                  name="{photoCalibName}",
                                                  multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-tract measurement.",
                                                  dimensions=("tract", "skymap",
                                                              "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class TractMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                          pipelineConnections=TractMeasTaskConnections):
    pass


class TractMeasTask(CatalogMeasureBaseTask):

    ConfigClass = TractMeasTaskConfig
    _DefaultName = "tractMeasTask"

    def run(self, cat, photo_calibs, vIds):
        return self.measure.run(cat, photo_calibs, self.config.connections.metric, vIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.cat]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)


class TractMultiBandMeasTaskConnections(TractMeasTaskConnections,
                                        dimensions=("tract", "skymap"),
                                        defaultTemplates={"coaddName": "deepCoadd", "photoCalibName":
                                                          "deepCoadd_calexp.photoCalib"}):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "skymap", "patch",
                                                     "band"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src",
                                         multiple=True)

    photo_calibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                  dimensions=("tract", "skymap",
                                                              "patch", "band"),
                                                  storageClass="PhotoCalib",
                                                  name="{photoCalibName}",
                                                  multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-tract measurement.",
                                                  dimensions=("tract", "skymap"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class TractMultiBandMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                                   pipelineConnections=TractMultiBandMeasTaskConnections):
    pass


class TractMultiBandMeasTask(TractMeasTask):

    ConfigClass = TractMultiBandMeasTaskConfig
    _DefaultName = "tractMultiBandMeasTask"
