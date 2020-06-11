import astropy.units as u
import numpy as np

from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement

class PatchAggregationTaskConnections(MetricConnections,
                                      defaultTemplates={'agg_name': None},
                                      dimensions=("skymap", "abstract_filter")):
    
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch", "skymap", 
                                                              "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)
    
    measurement = pipeBase.connectionTypes.Output(doc="{agg_name} {package}_{metric}.",
                                                  dimensions=("skymap", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{agg_name}_{package}_{metric}")
    
class PatchAggregationTaskConfig(MetricConfig,
                                 pipelineConnections=PatchAggregationTaskConnections):
    pass

class PatchAggregationTask(MetricTask):

    ConfigClass = PatchAggregationTaskConfig
    _DefaultName = "patchAggregationTask"

    def run(self, measurements):
        package = self.config.connections.package
        metric = self.config.connections.metric
        agg = self.config.connections.agg_name.lower()
        self.log.info(f"Computing the {agg} of {package}_{metric} values in patch catalogs")

        value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return pipeBase.Struct(measurement=Measurement(f"metricvalue_{agg}_{package}_{metric}", value))