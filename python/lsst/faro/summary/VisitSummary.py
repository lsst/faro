import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (
    CatalogSummaryBaseTaskConnections,
    CatalogSummaryBaseTask,
    CatalogSummaryBaseConfig,
)

__all__ = ("VisitSummaryTaskConnections", "VisitSummaryConfig", "VisitSummaryTask")


class VisitSummaryTaskConnections(CatalogSummaryBaseTaskConnections):

    measurements = pipeBase.connectionTypes.Input(
        doc="{package}_{metric}.",
        dimensions=("instrument", "visit", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
        multiple=True,
    )


class VisitSummaryConfig(
    CatalogSummaryBaseConfig, pipelineConnections=VisitSummaryTaskConnections
):
    pass


class VisitSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = VisitSummaryConfig
    _DefaultName = "visitSummaryTask"
