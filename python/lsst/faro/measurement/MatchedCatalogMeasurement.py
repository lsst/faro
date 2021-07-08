import traceback

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections, MetricComputationError

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("PatchMatchedMeasurementTaskConnections", "PatchMatchedMeasurementTaskConfig",
           "PatchMatchedMeasurementTask",
           "TractMatchedMeasurementTaskConnections", "TractMatchedMeasurementTaskConfig",
           "TractMatchedMeasurementTask",
           "PatchMatchedMultiBandMeasurementTaskConnections", "PatchMatchedMultiBandMeasurementTaskConfig",
           "PatchMatchedMultiBandMeasurementTask")

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class PatchMatchedMeasurementTaskConnections(MetricConnections,
                                             dimensions=("tract", "patch", "band",
                                                         "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument",
                                                     "band"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogPatch")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class PatchMatchedMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                        pipelineConnections=PatchMatchedMeasurementTaskConnections):
    pass


class PatchMatchedMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = PatchMatchedMeasurementTaskConfig
    _DefaultName = "patchMatchedMeasurementTask"


class TractMatchedMeasurementTaskConnections(PatchMatchedMeasurementTaskConnections,
                                             dimensions=("tract", "instrument",
                                                         "band", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "instrument",
                                                     "band"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogTract")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class TractMatchedMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                        pipelineConnections=TractMatchedMeasurementTaskConnections):
    pass


class TractMatchedMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = TractMatchedMeasurementTaskConfig
    _DefaultName = "tractMatchedMeasurementTask"


class PatchMatchedMultiBandMeasurementTaskConnections(MetricConnections,
                                                      dimensions=("tract", "patch", "band",
                                                                  "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogPatchMultiBand")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class PatchMatchedMultiBandMeasurementTaskConfig(
        CatalogMeasurementBaseTaskConfig,
        pipelineConnections=PatchMatchedMultiBandMeasurementTaskConnections):
    pass


class PatchMatchedMultiBandMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = PatchMatchedMultiBandMeasurementTaskConfig
    _DefaultName = "patchMatchedMultiBandMeasurementTask"

    def run(self, cat, in_id, out_id):
        return self.measure.run(cat, self.config.connections.metric, in_id, out_id)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        """Do Butler I/O to provide in-memory objects for run.
        This specialization of runQuantum performs error-handling specific to
        MetricTasks. Most or all of this functionality may be moved to
        activators in the future.
        """
        try:
            in_id = butlerQC.registry.expandDataId(inputRefs.cat.dataId)
            out_id = butlerQC.registry.expandDataId(outputRefs.measurement.dataId)
            inputs = butlerQC.get(inputRefs)
            inputs['in_id'] = in_id
            inputs['out_id'] = out_id
            outputs = self.run(**inputs)
            if outputs.measurement is not None:
                butlerQC.put(outputs, outputRefs)
            else:
                self.log.debug("Skipping measurement of {!r} on {} "
                               "as not applicable.", self, inputRefs)
        except MetricComputationError as e:
            self.log.error("Measurement of {!r} failed on {}->{}\n{}\n,%s",
                           self, inputRefs, outputRefs, traceback.format_exc(), e.msg)
