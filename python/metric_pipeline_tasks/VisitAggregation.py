from lsst.verify.tasks import MetricConfig
import lsst.pipe.base as pipeBase

from .CatalogsAggregationBase import (CatalogsAggregationBaseTaskConnections, CatalogsAggregationBaseTask,
                                      CatalogAggregationBaseTaskConfig)

class VisitAggregationTaskConnections(CatalogsAggregationBaseTaskConnections):
    
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("instrument", "visit", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)
    
class VisitAggregationTaskConfig(CatalogAggregationBaseTaskConfig,
                                 pipelineConnections=VisitAggregationTaskConnections):
    pass

class VisitAggregationTask(CatalogsAggregationBaseTask):

    ConfigClass = VisitAggregationTaskConfig
    _DefaultName = "visitAggregationTask"
