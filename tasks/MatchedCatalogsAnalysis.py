import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
from MeasureTasks import measure_task_map

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class MatchedCatalogAnalysisTaskConnections(MetricConnections,
                                    dimensions=("tract", "patch", "abstract_filter",
                                                "instrument", "skymap")):
    matchedCatalog = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                                    dimensions=("tract", "patch", "instrument",
                                                                "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument","abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedCatalogAnalysisTaskConfig(MetricConfig,
                               defaultTemplates={"metric": None},
                               pipelineConnections=MatchedCatalogAnalysisTaskConnections):
    measure = pexConfig.ConfigurableField(
        target=measure_task_map["nsrcMeas"],
        doc="Measure task")


class MatchedCatalogAnalysisTask(MetricTask):

    ConfigClass = MatchedCatalogAnalysisTaskConfig
    _DefaultName = "matchedCatalogAnalysisTask"
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('measure')

    def run(self, matchedCatalog):
        return self.measure.run(matchedCatalog)
