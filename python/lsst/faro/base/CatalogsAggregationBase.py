from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig

from .BaseSubTasks import NumpyAggTask


# Dimentions of the Connections class define the iterations of runQuantum
class CatalogsAggregationBaseTaskConnections(MetricConnections,
                                             defaultTemplates={'agg_name': None},
                                             dimensions=("band", "tract",
                                                         "instrument", "skymap")):
    # Make this an LSST verify Measurement
    measurement = pipeBase.connectionTypes.Output(doc="{agg_name} {package}_{metric}.",
                                                  dimensions=("instrument", "tract", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{agg_name}_{package}_{metric}")


class CatalogAggregationBaseTaskConfig(MetricConfig,
                                       pipelineConnections=CatalogsAggregationBaseTaskConnections):
    agg = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumpyAggTask,
        doc="Numpy aggregation task")


class CatalogsAggregationBaseTask(MetricTask):

    ConfigClass = CatalogAggregationBaseTaskConfig
    _DefaultName = "catalogsAggregationBaseTask"

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('agg')

    def run(self, measurements):
        return self.agg.run(measurements, self.config.connections.agg_name, self.config.connections.package,
                            self.config.connections.metric)
