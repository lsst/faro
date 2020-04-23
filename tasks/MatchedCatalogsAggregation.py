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

import lsst.pipe.base as pipeBase
from lsst.verify import Measurement

# Dimentions of the Connections class define the iterations of runQuantum
class MatchedCatalogsAggregationTaskConnections(pipeBase.PipelineTaskConnections,
                                                dimensions=("abstract_filter", "instrument", "skymap")):
    measurements = pipeBase.connectionTypes.Input(doc="PA1.",
                                                  dimensions=("tract", "patch", "instrument", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="PA1",
                                                  multiple=True)
    # Make this an LSST verify Measurement
    summary = pipeBase.connectionTypes.Output(doc="Mean PA1.",
                                              dimensions=("instrument", "abstract_filter"),
                                              storageClass="MetricValue",
                                              name="meanPA1")

class MatchedCatalogsAggregationTaskConfig(pipeBase.PipelineTaskConfig,
                                           pipelineConnections=MatchedCatalogsAggregationTaskConnections):
    pass

class MatchedCatalogsAggregationTask(pipeBase.PipelineTask):

    ConfigClass = MatchedCatalogsAggregationTaskConfig
    _DefaultName = "matchedCatalogsAggregationTask"

    # Accepts python objects
    def run(self, measurements):
        self.log.info(f"Computing the mean of PA1 values in matched catalogs")

        value = np.mean(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return pipeBase.Struct(summary=Measurement("MeanPA1", value))
