import traceback

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections, MetricComputationError

from lsst.faro.base.CatalogMeasureBase import CatalogMeasureBaseTaskConfig, CatalogMeasureBaseTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class PatchMatchedMeasTaskConnections(MetricConnections,
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


class PatchMatchedMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                                 pipelineConnections=PatchMatchedMeasTaskConnections):
    pass


class PatchMatchedMeasTask(CatalogMeasureBaseTask):
    ConfigClass = PatchMatchedMeasTaskConfig
    _DefaultName = "patchMatchedMeasTask"


class TractMatchedMeasTaskConnections(PatchMatchedMeasTaskConnections,
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


class TractMatchedMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                                 pipelineConnections=TractMatchedMeasTaskConnections):
    pass


class TractMatchedMeasTask(CatalogMeasureBaseTask):
    ConfigClass = TractMatchedMeasTaskConfig
    _DefaultName = "tractMatchedMeasTask"


class PatchMatchedMultiBandMeasTaskConnections(MetricConnections,
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


class PatchMatchedMultiBandMeasTaskConfig(CatalogMeasureBaseTaskConfig,
                                          pipelineConnections=PatchMatchedMultiBandMeasTaskConnections):
    pass


class PatchMatchedMultiBandMeasTask(CatalogMeasureBaseTask):
    ConfigClass = PatchMatchedMultiBandMeasTaskConfig
    _DefaultName = "patchMatchedMultiBandMeasTask"

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
                self.log.debugf("Skipping measurement of {!r} on {} "
                                "as not applicable.", self, inputRefs)
        except MetricComputationError:
            # Apparently lsst.log doesn't have built-in exception support?
            self.log.errorf(
                "Measurement of {!r} failed on {}->{}\n{}",
                self, inputRefs, outputRefs, traceback.format_exc())
