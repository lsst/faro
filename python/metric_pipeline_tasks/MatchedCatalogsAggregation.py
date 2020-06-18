"""
# First run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i nSrc1 --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i pipeTest --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1

# After the first run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1 --replace-run

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1 --replace-run
"""

from lsst.verify.tasks import MetricConfig
import lsst.pipe.base as pipeBase

from .CatalogsAggregationBase import CatalogsAggregationBaseTaskConnections, CatalogsAggregationBaseTask

# Dimentions of the Connections class define the iterations of runQuantum
class MatchedCatalogsAggregationTaskConnections(CatalogsAggregationBaseTaskConnections):
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch", "instrument", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)

class MatchedCatalogsAggregationTaskConfig(MetricConfig,
                                           pipelineConnections=MatchedCatalogsAggregationTaskConnections):
    pass

class MatchedCatalogsAggregationTask(CatalogsAggregationBaseTask):

    ConfigClass = MatchedCatalogsAggregationTaskConfig
    _DefaultName = "matchedCatalogsAggregationTask"
