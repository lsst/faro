import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = ("PatchSummaryConnections", "PatchSummaryConfig", "PatchSummaryTask")


class PatchSummaryConnections(CatalogSummaryBaseConnections):

    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class PatchSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=PatchSummaryConnections
):
    pass


class PatchSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchSummaryConfig
    _DefaultName = "patchSummaryTask"
