import astropy.units as u
import numpy as np
import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom
from lsst.verify import Measurement
from lsst.verify.gen2tasks import register
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections, \
    MetricComputationError

from sst_metrics_utils.matcher import match_catalogs

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class MatchedCatalogsAnalysisTaskConnections(pipeBase.PipelineTaskConnections,
                                             dimensions=("tract", "patch", "abstract_filter", "instrument", "skymap")):
    matchedCatalog = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                                    dimensions=("tract", "patch", "instrument", "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")
    measurements = pipeBase.connectionTypes.Output(doc="Number of sources in catalog.",
                                                   dimensions=("tract", "patch", "instrument", "abstract_filter"),
                                                   storageClass="NumpyArray",
                                                   name="nsrcMeas")


class MatchedCatalogsAnalysisTaskConfig(pipeBase.PipelineTaskConfig,
                                        pipelineConnections=MatchedCatalogsAnalysisTaskConnections):
    pass


class MatchedCatalogsAnalysisTask(pipeBase.PipelineTask):

    ConfigClass = MatchedCatalogsAnalysisTaskConfig
    _DefaultName = "matchedCatalogsAnalysisTask"

#    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
#        super().__init__(*args, config=config, **kwargs)

    def run(self, matchedCatalog):
        self.log.info(f"Counting sources in matched catalog")
        nSources = len(matchedCatalog)
        meas = np.array(nSources)
#        meas = Measurement("numSources", nSources * u.count)
        return pipeBase.Struct(measurements=meas)

#    def runQuantum(self, butlerQC,
#                   inputRefs,
#                   outputRefs):
#        inputs = butlerQC.get(inputRefs)
#        inputs['vIds'] = [el.dataId.byName() for el in inputRefs.__dict__['source_catalogs']]
#        outputs = self.run(**inputs)
#        butlerQC.put(outputs, outputRefs)
