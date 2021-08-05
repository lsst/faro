import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = (
    "PatchMatchedSummaryConnections",
    "PatchMatchedSummaryConfig",
    "PatchMatchedSummaryTask",
    "TractMatchedSummaryConnections",
    "TractMatchedSummaryConfig",
    "TractMatchedSummaryTask",
)


# Dimensions of the Connections class define the iterations of runQuantum
class PatchMatchedSummaryConnections(CatalogSummaryBaseConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "patch", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class PatchMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=PatchMatchedSummaryConnections
):
    pass


class PatchMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchMatchedSummaryConfig
    _DefaultName = "patchMatchedSummaryTask"


class TractMatchedSummaryConnections(CatalogSummaryBaseConnections):
    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "instrument", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class TractMatchedSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=TractMatchedSummaryConnections
):
    pass


class TractMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = TractMatchedSummaryConfig
    _DefaultName = "tractMatchedSummaryTask"
