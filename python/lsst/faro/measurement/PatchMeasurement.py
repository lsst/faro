import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from lsst.faro.base.CatalogMeasureBase import CatalogMeasureBaseTaskConfig, CatalogMeasureBaseTask

__all__ = ("PatchMeasTaskConnections", "PatchMeasTaskConfig", "PatchMeasTask")


class PatchMeasTaskConnections(MetricConnections,
                               dimensions=("tract", "patch", "skymap",
                                           "band")):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "patch", "skymap",
                                                     "band"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src")

    measurement = pipeBase.connectionTypes.Output(doc="Per-patch measurement.",
                                                  dimensions=("tract", "patch", "skymap",
                                                              "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class PatchMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                          pipelineConnections=PatchMeasTaskConnections):
    pass


class PatchMeasTask(CatalogMeasureBaseTask):

    ConfigClass = PatchMeasTaskConfig
    _DefaultName = "patchMeasTask"

    def run(self, cat, vIds):
        return self.measure.run(cat, self.config.connections.metric, vIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['vIds'] = inputRefs.cat.dataId
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
