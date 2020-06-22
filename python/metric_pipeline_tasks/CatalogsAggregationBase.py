import astropy.units as u
import numpy as np

from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement

# Dimentions of the Connections class define the iterations of runQuantum
class CatalogsAggregationBaseTaskConnections(MetricConnections,
                                                defaultTemplates={'agg_name': None},
                                                dimensions=("abstract_filter", "instrument", "skymap")):
    # Make this an LSST verify Measurement
    measurement = pipeBase.connectionTypes.Output(doc="{agg_name} {package}_{metric}.",
                                              dimensions=("instrument", "abstract_filter"),
                                              storageClass="MetricValue",
                                              name="metricvalue_{agg_name}_{package}_{metric}")


class CatalogsAggregationBaseTask(MetricTask):

    ConfigClass = MetricConfig
    _DefaultName = "catalogsAggregationBaseTask"

    def run(self, measurements):
        package = self.config.connections.package
        metric = self.config.connections.metric
        agg = self.config.connections.agg_name.lower()
        self.log.info(f"Computing the {agg} of {package}_{metric} values")

        value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return pipeBase.Struct(measurement=Measurement(f"metricvalue_{agg}_{package}_{metric}", value))
