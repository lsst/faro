import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (CatalogSummaryBaseTaskConnections, CatalogSummaryBaseTask,
                                               CatalogSummaryBaseTaskConfig)


class VisitSummaryTaskConnections(CatalogSummaryBaseTaskConnections):

    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("instrument", "visit", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)


class VisitSummaryTaskConfig(CatalogSummaryBaseTaskConfig,
                             pipelineConnections=VisitSummaryTaskConnections):
    pass


class VisitSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = VisitSummaryTaskConfig
    _DefaultName = "visitSummaryTask"
