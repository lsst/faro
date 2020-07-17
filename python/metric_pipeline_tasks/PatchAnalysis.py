import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from .CatalogsAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchAnalysisTaskConnections(MetricConnections,
                                   dimensions=("tract", "patch", "skymap",
                                               "abstract_filter")):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "patch", "skymap",
                                                     "abstract_filter"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src")

    measurement = pipeBase.connectionTypes.Output(doc="Per-patch measurement.",
                                                  dimensions=("tract", "patch", "skymap",
                                                              "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class PatchAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                              pipelineConnections=PatchAnalysisTaskConnections):
    pass


class PatchAnalysisTask(CatalogAnalysisBaseTask):

    ConfigClass = PatchAnalysisTaskConfig
    _DefaultName = "patchAnalysisTask"
