import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from .CatalogsAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class MatchedCatalogAnalysisTaskConnections(MetricConnections,
                                            dimensions=("tract", "patch", "band",
                                                        "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument",
                                                     "band"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalog")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedCatalogAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                       pipelineConnections=MatchedCatalogAnalysisTaskConnections):
    pass


class MatchedCatalogAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedCatalogAnalysisTaskConfig
    _DefaultName = "matchedCatalogAnalysisTask"


class MatchedCatalogTractAnalysisTaskConnections(MatchedCatalogAnalysisTaskConnections,
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


class MatchedCatalogTractAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                            pipelineConnections=MatchedCatalogTractAnalysisTaskConnections):
    pass


class MatchedCatalogTractAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedCatalogTractAnalysisTaskConfig
    _DefaultName = "matchedCatalogTractAnalysisTask"


class MatchedMultiCatalogAnalysisTaskConnections(MetricConnections,
                                                 dimensions=("tract", "patch", "band",
                                                             "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogMulti")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedMultiCatalogAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                            pipelineConnections=MatchedMultiCatalogAnalysisTaskConnections):
    pass


class MatchedMultiCatalogAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedMultiCatalogAnalysisTaskConfig
    _DefaultName = "matchedMultiCatalogAnalysisTask"

    def run(self, cat, in_id, out_id):
        return self.measure.run(cat, self.config.connections.metric, in_id, out_id)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        """Do Butler I/O to provide in-memory objects for run.
        This specialization of runQuantum performs error-handling specific to
        MetricTasks. Most or all of this functionality may be moved to
        activators in the future.
        """
        # Synchronize changes to this method with ApdbMetricTask
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
