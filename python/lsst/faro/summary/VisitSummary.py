import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = ("VisitSummaryConnections", "VisitSummaryConfig", "VisitSummaryTask")


class VisitSummaryConnections(CatalogSummaryBaseConnections):

    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("instrument", "visit", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class VisitSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=VisitSummaryConnections
):
    pass


class VisitSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = VisitSummaryConfig
    _DefaultName = "visitSummaryTask"
