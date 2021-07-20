import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("VisitTableMeasurementConfig", "VisitTableMeasurementTask")


class VisitTableMeasurementConnections(MetricConnections,
                                       dimensions=("instrument", "visit", "band")):

    catalog = pipeBase.connectionTypes.Input(doc="Source catalog for visit.",
                                             dimensions=("instrument", "visit", "band"),
                                             storageClass="DataFrame",
                                             name="sourceTable_visit",
                                             deferLoad=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class VisitTableMeasurementConfig(CatalogMeasurementBaseTaskConfig,
                                  pipelineConnections=VisitTableMeasurementConnections):
    columns = pexConfig.Field(doc="Columns from sourceTable_visit to load.",
                              dtype=str, default='coord_ra, coord_dec')


class VisitTableMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitTableMeasurementConfig
    _DefaultName = "visitTableMeasurementTask"

    def run(self, catalog, dataIds):
        return self.measure.run(catalog, self.config.connections.metric, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        columns = [_.strip() for _ in self.config.columns.split(',')]
        inputs['catalog'] = inputs['catalog'].get(parameters={'columns': columns})
        inputs['dataIds'] = [butlerQC.registry.expandDataId(inputRefs.catalog.datasetRef.dataId)]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debugf("Skipping measurement of {!r} on {} "
                            "as not applicable.", self, inputRefs)
