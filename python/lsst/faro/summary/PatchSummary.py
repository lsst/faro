import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseTaskConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = ("PatchSummaryTaskConnections", "PatchSummaryConfig", "PatchSummaryTask")


class PatchSummaryTaskConnections(CatalogSummaryBaseTaskConnections):

    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("tract", "patch", "skymap", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class PatchSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=PatchSummaryTaskConnections
):
    pass


class PatchSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchSummaryConfig
    _DefaultName = "patchSummaryTask"
