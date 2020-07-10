import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
from lsst.afw.table import SourceCatalog

from .CatalogsAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class VisitAnalysisTaskConnections(MetricConnections,
                                   dimensions=("instrument", "visit", "abstract_filter")):

    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs.",
                                                     dimensions=("instrument", "visit",
                                                                 "detector", "abstract_filter"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class VisitAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                              pipelineConnections=VisitAnalysisTaskConnections):
    pass


class VisitAnalysisTask(CatalogAnalysisBaseTask):
    ConfigClass = VisitAnalysisTaskConfig
    _DefaultName = "visitAnalysisTask"

    def run(self, source_catalogs):

        # Concatenate catalogs
        schema = source_catalogs[0].schema
        size = sum([len(cat) for cat in source_catalogs])
        source_catalog = SourceCatalog(schema)
        source_catalog.reserve(size)
        for cat in source_catalogs:
            source_catalog.extend(cat)

        return self.measure.run(source_catalog, self.config.connections.metric)
