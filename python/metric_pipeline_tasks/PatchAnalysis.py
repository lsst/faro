import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections

from .CatalogsAnalysisBase import CatalogAnalysisBaseTaskConfig, CatalogAnalysisBaseTask


# The first thing to do is to define a Connections class. This will define all
# the inputs and outputs that our task requires
class PatchAnalysisTaskConnections(MetricConnections,
                                   dimensions=("tract", "patch", "skymap",
                                               "abstract_filter")):

    cat = pipeBase.connectionTypes.Input(doc="Object catalog.",
                                         dimensions=("tract", "patch", "skymap",
                                                     "abstract_filter"),
                                         storageClass="SourceCatalog",
                                         name="deepCoadd_forced_src")

    measurement = pipeBase.connectionTypes.Output(doc="Per-patch measurement.",
                                                  dimensions=("tract", "patch", "skymap",
                                                              "abstract_filter"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class PatchAnalysisTaskConfig(CatalogAnalysisBaseTaskConfig,
                              pipelineConnections=PatchAnalysisTaskConnections):
    pass


class PatchAnalysisTask(CatalogAnalysisBaseTask):

    ConfigClass = PatchAnalysisTaskConfig
    _DefaultName = "patchAnalysisTask"

    def run(self, cat, vIds):

#        # Concatenate catalogs
#        schema = source_catalogs[0].schema
#        size = sum([len(cat) for cat in source_catalogs])
#        source_catalog = SourceCatalog(schema)
#        source_catalog.reserve(size)
#        for cat in source_catalogs:
#            source_catalog.extend(cat)
#
        return self.measure.run(cat, self.config.connections.metric, vIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        # import pdb
        # pdb.set_trace()
        inputs['vIds'] = inputRefs.cat.dataId
        # [butlerQC.registry.expandDataId(el.dataId) for el in inputRefs.source_catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
