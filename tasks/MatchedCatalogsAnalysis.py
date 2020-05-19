import astropy.units as u
import numpy as np
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement
import lsst.pex.config as pexConfig
from sst_metrics_utils.filtermatches import filterMatches
from lsst.verify.gen2tasks import register
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections, \
    MetricComputationError
from lsst.validate.drp.repeatability import calcPhotRepeat

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class NumSourcesTaskConnections(pipeBase.PipelineTaskConnections,
                                dimensions=("tract", "patch", "abstract_filter",
                                            "instrument", "skymap")):
    matchedCatalog = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                                    dimensions=("tract", "patch", "instrument",
                                                                "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")
    measurements = pipeBase.connectionTypes.Output(doc="Number of sources in catalog.",
                                                   dimensions=("tract", "patch", "instrument",
                                                               "abstract_filter"),
                                                   storageClass="MetricValue",
                                                   name="nsrcMeas")


class NumSourcesTaskConfig(pipeBase.PipelineTaskConfig,
                           pipelineConnections=NumSourcesTaskConnections):
    pass


class NumSourcesTask(pipeBase.PipelineTask):

    ConfigClass = NumSourcesTaskConfig
    _DefaultName = "numSourcesTask"

    def run(self, matchedCatalog):
        self.log.info(f"Counting sources in matched catalog")
        nSources = len(matchedCatalog)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return pipeBase.Struct(measurements=meas)


class MeasurePA1TaskConnections(pipeBase.PipelineTaskConnections,
                                dimensions=("tract", "patch", "abstract_filter",
                                            "instrument", "skymap")):
    matchedCatalog = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                                    dimensions=("tract", "patch", "instrument",
                                                                "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")
    measurements = pipeBase.connectionTypes.Output(doc="PA1.",
                                                   dimensions=("tract", "patch", "instrument",
                                                               "abstract_filter"),
                                                   storageClass="MetricValue",
                                                   name="PA1")


class MeasurePA1TaskConfig(pipeBase.PipelineTaskConfig,
                           pipelineConnections=MeasurePA1TaskConnections):
    brightSnrMin = pexConfig.Field(doc="Minimum median SNR for a source to be considered bright.",
                                   dtype=float, default=50)
    brightSnrMax = pexConfig.Field(doc="Maximum median SNR for a source to be considered bright.",
                                   dtype=float, default=np.Inf)


class MeasurePA1Task(pipeBase.PipelineTask):

    ConfigClass = MeasurePA1TaskConfig
    _DefaultName = "measurePA1Task"

    def __init__(self, config: pipeBase.PipelineTaskConfig, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.brightSnrMin = self.config.brightSnrMin
        self.brightSnrMax = self.config.brightSnrMax

    def run(self, matchedCatalog):
        self.log.info(f"Measuring PA1")

        filteredCat = filterMatches(matchedCatalog) #, extended=False, isPrimary=False)
        magKey = filteredCat.schema.find('slot_PsfFlux_mag').key

        # Require at least nMinPA1=10 objects to calculate the repeatability:
        nMinPA1 = 50
        if filteredCat.count > nMinPA1:
            pa1 = calcPhotRepeat(filteredCat, magKey)
            return pipeBase.Struct(measurements=Measurement("PA1", pa1['repeatability']))
        else:
            return pipeBase.Struct(measurements=Measurement("PA1", np.nan*u.mmag))
