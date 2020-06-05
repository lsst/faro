import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections

from VisitMeasureTasks import NumSourcesTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires

class VisitAnalysisTaskConnections(MetricConnections,
                                   dimensions=("instrument", "visit", "detector", "abstract_filter")):
    
    source_catalog = pipeBase.connectionTypes.Input(doc="Source catalogs.",
                                                    dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                    storageClass="SourceCatalog",
                                                    name="src")
                                                    #multiple=True) # We might need to compile results by visit
    
    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")
    
class VisitAnalysisTaskConfig(MetricConfig,
                              pipelineConnections=VisitAnalysisTaskConnections):
    measure = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumSourcesTask,
        doc="Measure task")
    
class VisitAnalysisTask(MetricTask):

    ConfigClass = VisitAnalysisTaskConfig
    _DefaultName = "visitAnalysisTask"
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('measure')

    def run(self, source_catalog):
        return self.measure.run(source_catalog, self.config.connections.metric)