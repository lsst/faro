import lsst.pipe.base as pipeBase

from .MatchedCatalogsBase import (MatchedBaseTaskConnections,
                                  MatchedBaseTaskConfig,
                                  MatchedBaseTask, MatchedTractBaseTask)


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class MatchedCatalogTaskConnections(MatchedBaseTaskConnections,
                                    dimensions=("tract", "patch", "abstract_filter",
                                                "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument", "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")


class MatchedCatalogTaskConfig(MatchedBaseTaskConfig,
                               pipelineConnections=MatchedCatalogTaskConnections):
    pass


class MatchedCatalogTask(MatchedBaseTask):

    ConfigClass = MatchedCatalogTaskConfig
    _DefaultName = "matchedCatalogTask"


class MatchedCatalogTractTaskConnections(MatchedBaseTaskConnections,
                                         dimensions=("tract", "abstract_filter",
                                                     "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract",
                                                                "instrument", "abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogTract")


class MatchedCatalogTractTaskConfig(MatchedBaseTaskConfig,
                                    pipelineConnections=MatchedCatalogTractTaskConnections):
    pass


class MatchedCatalogTractTask(MatchedTractBaseTask):

    ConfigClass = MatchedCatalogTractTaskConfig
    _DefaultName = "matchedCatalogTractTask"


class MatchedCatalogMultiTaskConnections(MatchedBaseTaskConnections,
                                         dimensions=("tract", "patch", "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogMulti")


class MatchedCatalogMultiTaskConfig(MatchedBaseTaskConfig,
                                    pipelineConnections=MatchedCatalogMultiTaskConnections):
    pass


class MatchedCatalogMultiTask(MatchedBaseTask):

    ConfigClass = MatchedCatalogMultiTaskConfig
    _DefaultName = "matchedCatalogMultiTask"
