from lsst.verify.tasks import MetricConfig
import lsst.pipe.base as pipeBase

from .CatalogsAggregationBase import CatalogsAggregationBaseTaskConnections, CatalogsAggregationBaseTask

class PatchAggregationTaskConnections(CatalogsAggregationBaseTaskConnections):
    
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch", "skymap", 
                                                              "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)
    
class PatchAggregationTaskConfig(MetricConfig,
                                 pipelineConnections=PatchAggregationTaskConnections):
    pass

class PatchAggregationTask(CatalogsAggregationBaseTask):

    ConfigClass = PatchAggregationTaskConfig
    _DefaultName = "patchAggregationTask"
