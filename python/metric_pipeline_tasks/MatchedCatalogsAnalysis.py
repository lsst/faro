import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from .CatalogsAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires


class MatchedCatalogAnalysisTaskConnections(MetricConnections,
                                            dimensions=("tract", "patch", "band",
                                                        "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument",
                                                     "band"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalog")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedCatalogAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                       pipelineConnections=MatchedCatalogAnalysisTaskConnections):
    pass


class MatchedCatalogAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedCatalogAnalysisTaskConfig
    _DefaultName = "matchedCatalogAnalysisTask"


class MatchedCatalogTractAnalysisTaskConnections(MatchedCatalogAnalysisTaskConnections,
                                                 dimensions=("tract", "instrument",
                                                             "band", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "instrument",
                                                     "band"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogTract")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedCatalogTractAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                            pipelineConnections=MatchedCatalogTractAnalysisTaskConnections):
    pass


class MatchedCatalogTractAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedCatalogTractAnalysisTaskConfig
    _DefaultName = "matchedCatalogTractAnalysisTask"


class MatchedMultiCatalogAnalysisTaskConnections(MetricConnections,
                                                 dimensions=("tract", "patch", "band",
                                                             "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "patch", "instrument"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogMulti")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedMultiCatalogAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                            pipelineConnections=MatchedMultiCatalogAnalysisTaskConnections):
    pass


class MatchedMultiCatalogAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedMultiCatalogAnalysisTaskConfig
    _DefaultName = "matchedMultiCatalogAnalysisTask"


class MatchedMultiCatalogTractAnalysisTaskConnections(MetricConnections,
                                                      dimensions=("tract", "abstract_filter",
                                                                  "instrument", "skymap")):
    cat = pipeBase.connectionTypes.Input(doc="Input matched catalog.",
                                         dimensions=("tract", "instrument"),
                                         storageClass="SimpleCatalog",
                                         name="matchedCatalogTractMulti")
    measurement = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                  dimensions=("tract",
                                                              "instrument", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class MatchedMultiCatalogTractAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                                                 pipelineConnections=MatchedMultiCatalogTractAnalysisTaskConnections):
    pass


class MatchedMultiCatalogTractAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = MatchedMultiCatalogTractAnalysisTaskConfig
    _DefaultName = "matchedMultiCatalogTractAnalysisTask"
