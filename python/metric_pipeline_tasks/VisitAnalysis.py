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


class VisitAnalysisTaskWithFilt(CatalogAnalysisBaseTask):
    ConfigClass = VisitAnalysisTaskConfig
    _DefaultName = "visitAnalysisTaskWithFilt"

    def run(self, source_catalogs, vIds):

        # Concatenate catalogs
        schema = source_catalogs[0].schema
        size = sum([len(cat) for cat in source_catalogs])
        source_catalog = SourceCatalog(schema)
        source_catalog.reserve(size)
        for cat in source_catalogs:
            source_catalog.extend(cat)

        filtername = vIds[0]['abstract_filter']

        return self.measure.run(source_catalog, self.config.connections.metric, filtername)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        """Do Butler I/O to provide in-memory objects for run.
        This specialization of runQuantum performs error-handling specific to
        MetricTasks. Most or all of this functionality may be moved to
        activators in the future.
        """
        inputs = butlerQC.get(inputRefs)
        inputs['vIds'] = [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
