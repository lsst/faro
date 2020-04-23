import astropy.units as u
import numpy as np
import lsst.pipe.base as pipeBase
from lsst.verify import Measurement
from lsst.afw.table import GroupView
from lsst.verify.gen2tasks import register
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections, \
    MetricComputationError
from lsst.validate.drp.repeatability import calcPhotRepeat
from sst_metrics_utils.matcher import match_catalogs

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
    pass


class MeasurePA1Task(pipeBase.PipelineTask):

    ConfigClass = MeasurePA1TaskConfig
    _DefaultName = "measurePA1Task"

    def run(self, matchedCatalog):
        self.log.info(f"Measuring PA1")
        matchedCat = GroupView.build(matchedCatalog)
        magKey = matchedCat.schema.find('slot_PsfFlux_mag').key

        def nMatchFilter(cat):
            nMatchesRequired = 2
            if len(cat) < nMatchesRequired:
                return False
            return np.isfinite(cat.get(magKey)).all()

        if len(matchedCat.where(nMatchFilter)) > 0:
            pa1 = calcPhotRepeat(matchedCat.where(nMatchFilter), magKey)
            return pipeBase.Struct(measurements=Measurement("PA1", pa1['repeatability']))
        else:
            return pipeBase.Struct(measurements=Measurement("PA1", np.nan))
