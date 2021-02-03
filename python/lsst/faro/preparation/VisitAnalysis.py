import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
from lsst.afw.table import SourceCatalog

from lsst.faro.base.CatalogAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class VisitMeasTaskConnections(MetricConnections,
                               dimensions=("instrument", "visit", "band")):

    source_catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs.",
                                                     dimensions=("instrument", "visit",
                                                                 "detector", "band"),
                                                     storageClass="SourceCatalog",
                                                     name="src",
                                                     multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class VisitMeasTaskConfig(CatalogAnalysisBaseTaskConfig,
                          pipelineConnections=VisitMeasTaskConnections):
    pass


class VisitMeasTask(CatalogAnalysisBaseTask):
    ConfigClass = VisitMeasTaskConfig
    _DefaultName = "visitMeasTask"

    def run(self, source_catalogs, vIds):

        # Concatenate catalogs
        schema = source_catalogs[0].schema
        size = sum([len(cat) for cat in source_catalogs])
        source_catalog = SourceCatalog(schema)
        source_catalog.reserve(size)
        for cat in source_catalogs:
            source_catalog.extend(cat)

        return self.measure.run(source_catalog, self.config.connections.metric, vIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
