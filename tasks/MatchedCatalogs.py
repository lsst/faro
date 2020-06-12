import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.geom as geom

from MatchedCatalogsBase import (MatchedBaseTaskConnections,
                                 MatchedBaseTaskConfig,
                                 MatchedBaseTask)

# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class MatchedCatalogTaskConnections(MatchedBaseTaskConnections,
                                    dimensions=("tract", "patch", "abstract_filter", "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument","abstract_filter"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalog")


class MatchedCatalogTaskConfig(MatchedBaseTaskConfig,
                               pipelineConnections=MatchedCatalogTaskConnections):
    pass


class MatchedCatalogTask(MatchedBaseTask):

    ConfigClass = MatchedCatalogTaskConfig
    _DefaultName = "matchedCatalogTask"

# -------------------------------------------------------------------


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
