import lsst.pipe.base as pipeBase

from lsst/faro/base/MatchedCatalogBase import (MatchedBaseTaskConnections,
                                               MatchedBaseTaskConfig,
                                               MatchedBaseTask, MatchedTractBaseTask)


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchMatchedPrepTaskConnections(MatchedBaseTaskConnections,
                                      dimensions=("tract", "patch", "band",
                                                  "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument", "band"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogPatch")


class PatchMatchedPrepTaskConfig(MatchedBaseTaskConfig,
                                 pipelineConnections=PatchMatchedPrepTaskConnections):
    pass


class PatchMatchedPrepTask(MatchedBaseTask):

    ConfigClass = PatchMatchedPrepTaskConfig
    _DefaultName = "patchMatchedPrepTask"


class TractMatchedPrepTaskConnections(MatchedBaseTaskConnections,
                                      dimensions=("tract", "band",
                                                  "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "instrument", "band"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogTract")


class TractMatchedPrepTaskConfig(MatchedBaseTaskConfig,
                                 pipelineConnections=TractMatchedPrepTaskConnections):
    pass


class TractMatchedPrepTask(MatchedTractBaseTask):

    ConfigClass = TractMatchedPrepTaskConfig
    _DefaultName = "tractMatchedPrepTask"


class PatchMatchedMultiBandPrepTaskConnections(MatchedBaseTaskConnections,
                                               dimensions=("tract", "patch", "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogPatchMultiBand")


class PatchMatchedMultiBandPrepTaskConfig(MatchedBaseTaskConfig,
                                          pipelineConnections=PatchMatchedMultiBandPrepTaskConnections):
    pass


class PatchMatchedMultiBandPrepTask(MatchedBaseTask):

    ConfigClass = PatchMatchedMultiBandPrepTaskConfig
    _DefaultName = "patchMatchedMultiBandPrepTask"
