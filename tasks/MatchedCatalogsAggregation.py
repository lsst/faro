"""
# First run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i nSrc1 --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml -i pipeTest --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1

# After the first run
pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanNSrc1 --replace-run

pipetask run -j 1 -b "$CI_HSC_GEN3_DIR"/DATA/butler.yaml --register-dataset-types -t MatchedCatalogsAggregation.MatchedCatalogsAggregationTask -d "abstract_filter='r'" -o meanPA1 --replace-run
"""

import pdb
import astropy.units as u
import numpy as np

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify import Measurement
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections, \
    MetricComputationError

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

#    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
#        super().__init__(*args, config=config, **kwargs)

    # Accepts python objects
    def run(self, measurements):
        self.log.info(f"Computing the mean of PA1 values in matched catalogs")
        
        print(measurements)
        
        measurement_array = u.Quantity([x.quantity for x in measurements], u.mmag)
        print(measurement_array)
        print(np.mean(measurement_array))
        
        #measurement_array = []
        #for ii in range(0, len(measurements)):
        #    print(ii, measurements[ii])
        #    measurement_array.append()
        #meas = np.array(0.)
        #meas = Measurement("numSources", nSources * u.count)
        input('WAIT')
        return pipeBase.Struct(summary=Measurement("MeanPA1", np.mean(measurement_array)))

    # Use this to assemble python objects to send to the run method
    def runQuantum(self, butlerQC,
                   inputRefs,
                   outputRefs):
        inputs = butlerQC.get(inputRefs)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)
#        inputs['vIds'] = [el.dataId.byName() for el in inputRefs.__dict__['source_catalogs']]
#        outputs = self.run(**inputs)
#        butlerQC.put(outputs, outputRefs)