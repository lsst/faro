import lsst.pipe.base as pipeBase

from lsst.faro.base.MatchedCatalogBase import (
    MatchedBaseConnections,
    MatchedBaseConfig,
    MatchedBaseTask,
    MatchedTractBaseTask,
)

__all__ = (
    "PatchMatchedPreparationConnections",
    "PatchMatchedPreparationConfig",
    "PatchMatchedPreparationTask",
    "TractMatchedPreparationConnections",
    "TractMatchedPreparationConfig",
    "TractMatchedPreparationTask",
    "PatchMatchedMultiBandPreparationConnections",
    "PatchMatchedMultiBandPreparationConfig",
    "PatchMatchedMultiBandPreparationTask",
)


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchMatchedPreparationConnections(
    MatchedBaseConnections,
    dimensions=("tract", "patch", "band", "instrument", "skymap"),
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatch",
    )


class PatchMatchedPreparationConfig(
    MatchedBaseConfig, pipelineConnections=PatchMatchedPreparationConnections
):
    pass


class PatchMatchedPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedPreparationConfig
    _DefaultName = "patchMatchedPreparationTask"


class TractMatchedPreparationConnections(
    MatchedBaseConnections, dimensions=("tract", "band", "instrument", "skymap")
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "instrument", "band"),
        storageClass="SimpleCatalog",
        name="matchedCatalogTract",
    )


class TractMatchedPreparationConfig(
    MatchedBaseConfig, pipelineConnections=TractMatchedPreparationConnections
):
    pass


class TractMatchedPreparationTask(MatchedTractBaseTask):

    ConfigClass = TractMatchedPreparationConfig
    _DefaultName = "tractMatchedPreparationTask"


class PatchMatchedMultiBandPreparationConnections(
    MatchedBaseConnections, dimensions=("tract", "patch", "instrument", "skymap")
):
    outputCatalog = pipeBase.connectionTypes.Output(
        doc="Resulting matched catalog.",
        dimensions=("tract", "patch", "instrument"),
        storageClass="SimpleCatalog",
        name="matchedCatalogPatchMultiBand",
    )


class PatchMatchedMultiBandPreparationConfig(
    MatchedBaseConfig,
    pipelineConnections=PatchMatchedMultiBandPreparationConnections,
):
    pass


class PatchMatchedMultiBandPreparationTask(MatchedBaseTask):

    ConfigClass = PatchMatchedMultiBandPreparationConfig
    _DefaultName = "patchMatchedMultiBandPreparationTask"
