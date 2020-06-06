import astropy.units as u
import numpy as np

from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement

class VisitAggregationTaskConnections(MetricConnections,
                                      defaultTemplates={'agg_name': None},
                                      dimensions=("abstract_filter", "instrument")):
    
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("instrument", "visit", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)
    
    measurement = pipeBase.connectionTypes.Output(doc="{agg_name} {package}_{metric}.",
                                                  dimensions=("instrument", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{agg_name}_{package}_{metric}")
    
class VisitAggregationTaskConfig(MetricConfig,
                                 pipelineConnections=VisitAggregationTaskConnections):
    pass

class VisitAggregationTask(MetricTask):

    ConfigClass = VisitAggregationTaskConfig
    _DefaultName = "visitAggregationTask"

    def run(self, measurements):
        package = self.config.connections.package
        metric = self.config.connections.metric
        agg = self.config.connections.agg_name.lower()
        self.log.info(f"Computing the {agg} of {package}_{metric} values in single-visit catalogs")

        value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return pipeBase.Struct(measurement=Measurement(f"metricvalue_{agg}_{package}_{metric}", value))