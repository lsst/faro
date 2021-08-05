import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseTaskConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = (
    "PatchMatchedSummaryTaskConnections",
    "PatchMatchedSummaryConfig",
    "PatchMatchedSummaryTask",
    "TractMatchedSummaryTaskConnections",
    "TractMatchedSummaryConfig",
    "TractMatchedSummaryTask",
)


# Dimensions of the Connections class define the iterations of runQuantum
class PatchMatchedSummaryTaskConnections(CatalogSummaryBaseTaskConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class PatchMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=PatchMatchedSummaryTaskConnections
):
    pass


class PatchMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchMatchedSummaryConfig
    _DefaultName = "patchMatchedSummaryTask"


class TractMatchedSummaryTaskConnections(CatalogSummaryBaseTaskConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class TractMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=TractMatchedSummaryTaskConnections
):
    pass


class TractMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = TractMatchedSummaryConfig
    _DefaultName = "tractMatchedSummaryTask"
