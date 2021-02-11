import lsst.pipe.base as pipeBase

from lsst.faro.base.CatalogSummaryBase import (CatalogSummaryBaseTaskConnections, CatalogSummaryBaseTask,
                                               CatalogSummaryBaseTaskConfig)


class PatchSummaryTaskConnections(CatalogSummaryBaseTaskConnections):

    measurements = pipeBase.connectionTypes.Input(doc="{package}_{metric}.",
                                                  dimensions=("tract", "patch", "skymap",
                                                              "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}",
                                                  multiple=True)


class PatchSummaryTaskConfig(CatalogSummaryBaseTaskConfig,
                             pipelineConnections=PatchSummaryTaskConnections):
    pass


class PatchSummaryTask(CatalogSummaryBaseTask):

    ConfigClass = PatchSummaryTaskConfig
    _DefaultName = "patchSummaryTask"
