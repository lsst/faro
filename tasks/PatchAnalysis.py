import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
from lsst.afw.table import SourceCatalog

from GeneralMeasureTasks import NumSourcesTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchAnalysisTaskConnections(MetricConnections,
                                   dimensions=("tract", "patch", "skymap",
                                               "abstract_filter")):
    
    object_catalog = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                                    dimensions=("tract", "patch", "skymap", 
                                                                "abstract_filter"),
                                                    storageClass="SourceCatalog",
                                                    name="deepCoadd_forced_src")
    
    measurement = pipeBase.connectionTypes.Output(doc="Per-patch measurement.",
                                                  dimensions=("tract", "patch", "skymap",
                                                              "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")
    
class PatchAnalysisTaskConfig(MetricConfig,
                              pipelineConnections=PatchAnalysisTaskConnections):
    measure = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumSourcesTask,
        doc="Measure task")
    
class PatchAnalysisTask(MetricTask):

    ConfigClass = PatchAnalysisTaskConfig
    _DefaultName = "patchAnalysisTask"
    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('measure')

    def run(self, object_catalog):
        return self.measure.run(object_catalog, self.config.connections.metric)