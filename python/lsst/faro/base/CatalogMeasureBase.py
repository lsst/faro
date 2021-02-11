import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections

from .BaseSubTasks import NumSourcesTask


class CatalogMeasureBaseTaskConfig(MetricConfig,
                                   pipelineConnections=MetricConnections):
    measure = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumSourcesTask,
        doc="Measure task")


class CatalogMeasureBaseTask(MetricTask):

    ConfigClass = CatalogMeasureBaseTaskConfig
    _DefaultName = "catalogMeasureBaseTask"

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('measure')

    def run(self, cat):
        return self.measure.run(cat, self.config.connections.metric)
