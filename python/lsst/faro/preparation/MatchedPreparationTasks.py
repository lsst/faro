import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig

from lsst.faro.base.MatchedCatalogBase import (MatchedBaseTaskConnections,
                                               MatchedBaseTaskConfig,
                                               MatchedBaseTask, MatchedTractBaseTask)

__all__ = ("PatchMatchedPreparationTaskConnections", "PatchMatchedPreparationTaskConfig",
           "PatchMatchedPreparationTask",
           "TractMatchedPreparationTaskConnections", "TractMatchedPreparationTaskConfig",
           "TractMatchedPreparationTask",
           "PatchMatchedMultiBandPreparationTaskConnections", "PatchMatchedMultiBandPreparationTaskConfig",
           "PatchMatchedMultiBandPreparationTask")


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchMatchedPreparationTaskConnections(MatchedBaseTaskConnections,
                                             dimensions=("tract", "patch", "band",
                                                         "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument", "band"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogPatch")


class PatchMatchedPreparationTaskConfig(MatchedBaseTaskConfig,
                                        pipelineConnections=PatchMatchedPreparationTaskConnections):
    pass


class PatchMatchedPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedPreparationTaskConfig
    _DefaultName = "patchMatchedPreparationTask"


class TractMatchedPreparationTaskConnections(MatchedBaseTaskConnections,
                                             dimensions=("tract", "band",
                                                         "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "instrument", "band"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogTract")


class TractMatchedPreparationTaskConfig(MatchedBaseTaskConfig,
                                        pipelineConnections=TractMatchedPreparationTaskConnections):
    pass


class TractMatchedPreparationTask(MatchedTractBaseTask):

    ConfigClass = TractMatchedPreparationTaskConfig
    _DefaultName = "tractMatchedPreparationTask"


class PatchMatchedMultiBandPreparationTaskConnections(MatchedBaseTaskConnections,
                                                      dimensions=("tract", "patch", "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogPatchMultiBand")


class PatchMatchedMultiBandPreparationTaskConfig(
        MatchedBaseTaskConfig,
        pipelineConnections=PatchMatchedMultiBandPreparationTaskConnections):
    pass


class PatchMatchedMultiBandPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedMultiBandPreparationTaskConfig
    _DefaultName = "patchMatchedMultiBandPreparationTask"
