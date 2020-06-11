import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
from lsst.afw.table import SourceCatalog

from GeneralMeasureTasks import NumSourcesTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class VisitAnalysisTaskConnections(MetricConnections,
                                   dimensions=("instrument", "visit", "abstract_filter")):
    
    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs.",
                                                     dimensions=("instrument", "visit", "detector", "abstract_filter"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)
    
    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "abstract_filter"),
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

    def run(self, source_catalogs):
        
        # Concatenate catalogs
        schema = source_catalogs[0].schema
        size = sum([len(cat) for cat in source_catalogs])
        source_catalog = SourceCatalog(schema)
        source_catalog.reserve(size)
        for cat in source_catalogs:
            source_catalog.extend(cat)
        
        return self.measure.run(source_catalog, self.config.connections.metric)
