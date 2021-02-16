import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (CatalogSummaryBaseTaskConnections, CatalogSummaryBaseTask,
                                               CatalogSummaryBaseTaskConfig)

__all__ = ("PatchMatchedSummaryTaskConnections", "PatchMatchedSummaryTaskConfig", "PatchMatchedSummaryTask",
           "TractMatchedSummaryTaskConnections", "TractMatchedSummaryTaskConfig", "TractMatchedSummaryTask")


# Dimensions of the Connections class define the iterations of runQuantum
class PatchMatchedSummaryTaskConnections(CatalogSummaryBaseTaskConnections):
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch",
                                                              "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)


class PatchMatchedSummaryTaskConfig(CatalogSummaryBaseTaskConfig,
                                    pipelineConnections=PatchMatchedSummaryTaskConnections):
    pass


class PatchMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchMatchedSummaryTaskConfig
    _DefaultName = "patchMatchedSummaryTask"


class TractMatchedSummaryTaskConnections(CatalogSummaryBaseTaskConnections):
    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "instrument", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)


class TractMatchedSummaryTaskConfig(CatalogSummaryBaseTaskConfig,
                                    pipelineConnections=TractMatchedSummaryTaskConnections):
    pass


class TractMatchedSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = TractMatchedSummaryTaskConfig
    _DefaultName = "tractMatchedSummaryTask"
