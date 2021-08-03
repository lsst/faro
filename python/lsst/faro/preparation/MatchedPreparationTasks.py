import lsst.pipe.base as pipeBase

from lsst.faro.base.MatchedCatalogBase import (MatchedBaseTaskConnections,
                                               MatchedBaseConfig,
                                               MatchedBaseTask, MatchedTractBaseTask)

__all__ = ("PatchMatchedPreparationTaskConnections", "PatchMatchedPreparationConfig",
           "PatchMatchedPreparationTask",
           "TractMatchedPreparationTaskConnections", "TractMatchedPreparationConfig",
           "TractMatchedPreparationTask",
           "PatchMatchedMultiBandPreparationTaskConnections", "PatchMatchedMultiBandPreparationConfig",
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


class PatchMatchedPreparationConfig(MatchedBaseConfig,
                                        pipelineConnections=PatchMatchedPreparationTaskConnections):
    pass


class PatchMatchedPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedPreparationConfig
    _DefaultName = "patchMatchedPreparationTask"


class TractMatchedPreparationTaskConnections(MatchedBaseTaskConnections,
                                             dimensions=("tract", "band",
                                                         "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "instrument", "band"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogTract")


class TractMatchedPreparationConfig(MatchedBaseConfig,
                                        pipelineConnections=TractMatchedPreparationTaskConnections):
    pass


class TractMatchedPreparationTask(MatchedTractBaseTask):

    ConfigClass = TractMatchedPreparationConfig
    _DefaultName = "tractMatchedPreparationTask"


class PatchMatchedMultiBandPreparationTaskConnections(MatchedBaseTaskConnections,
                                                      dimensions=("tract", "patch", "instrument", "skymap")):
    outputCatalog = pipeBase.connectionTypes.Output(doc="Resulting matched catalog.",
                                                    dimensions=("tract", "patch",
                                                                "instrument"),
                                                    storageClass="SimpleCatalog",
                                                    name="matchedCatalogPatchMultiBand")


class PatchMatchedMultiBandPreparationConfig(
        MatchedBaseConfig,
        pipelineConnections=PatchMatchedMultiBandPreparationTaskConnections):
    pass


class PatchMatchedMultiBandPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedMultiBandPreparationConfig
    _DefaultName = "patchMatchedMultiBandPreparationTask"
