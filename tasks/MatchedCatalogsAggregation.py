"""
# First run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i nSrc1 --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i pipeTest --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1

# After the first run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1 --replace-run

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1 --replace-run
"""

import astropy.units as u
import numpy as np

from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement

# Dimentions of the Connections class define the iterations of runQuantum
class MatchedCatalogsAggregationTaskConnections(MetricConnections,
                                                defaultTemplates={'agg_name': None},
                                                dimensions=("abstract_filter", "instrument", "skymap")):
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch", "instrument", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)
    # Make this an LSST verify Measurement
    measurement = pipeBase.connectionTypes.Output(doc="{agg_name} {package}_{metric}.",
                                              dimensions=("instrument", "abstract_filter"),
                                              storageClass="MetricValue",
                                              name="metricvalue_{agg_name}_{package}_{metric}")

class MatchedCatalogsAggregationTaskConfig(MetricConfig,
                                           pipelineConnections=MatchedCatalogsAggregationTaskConnections):
    pass

class MatchedCatalogsAggregationTask(MetricTask):

    ConfigClass = MatchedCatalogsAggregationTaskConfig
    _DefaultName = "matchedCatalogsAggregationTask"

    # Accepts python objects
    def run(self, measurements):
        package = self.config.connections.package
        metric = self.config.connections.metric
        agg = self.config.connections.agg_name.lower()
        self.log.info(f"Computing the {agg} of {package}_{metric} values in matched catalogs")

        value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return pipeBase.Struct(measurement=Measurement("metricvalue_{agg}_{package}_{metric}", value))
