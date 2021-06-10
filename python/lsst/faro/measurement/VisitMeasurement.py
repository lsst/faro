import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask

__all__ = ("VisitMeasurementTaskConfig", "VisitMeasurementTask",
           "VisitTableMeasurementTaskConfig", "VisitTableMeasurementTask")


class VisitMeasurementTaskConnections(MetricConnections,
                                      dimensions=("instrument", "visit", "band"),
                                      defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                        "wcsName": "calexp.wcs"}):

    catalogs = pipeBase.connectionTypes.Input(doc="Source catalogs.",
                                              dimensions=("instrument", "visit",
                                                          "detector", "band"),
                                              storageClass="SourceCatalog",
                                              name="src",
                                              multiple=True)

    photoCalibs = pipeBase.connectionTypes.Input(doc="Photometric calibration object.",
                                                 dimensions=("instrument", "visit",
                                                             "detector", "band"),
                                                 storageClass="PhotoCalib",
                                                 name="{photoCalibName}",
                                                 multiple=True)

    astromCalibs = pipeBase.connectionTypes.Input(doc="WCS for the catalog.",
                                                  dimensions=("instrument", "visit",
                                                              "detector", "band"),
                                                  storageClass="Wcs",
                                                  name="{wcsName}",
                                                  multiple=True)

    measurement = pipeBase.connectionTypes.Output(doc="Per-visit measurement.",
                                                  dimensions=("instrument", "visit", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")


class VisitMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                 pipelineConnections=VisitMeasurementTaskConnections):
    pass


class VisitMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitMeasurementTaskConfig
    _DefaultName = "visitMeasurementTask"

    def run(self, catalogs, photoCalibs, astromCalibs, dataIds):
        return self.measure.run(self.config.connections.metric, catalogs, photoCalibs, astromCalibs, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['dataIds'] = [butlerQC.registry.expandDataId(c.dataId) for c in inputRefs.catalogs]
        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)


class VisitTableMeasurementTaskConnections(MetricConnections,
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


class VisitTableMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                      pipelineConnections=VisitTableMeasurementTaskConnections):
    columns = pexConfig.Field(doc="Columns from sourceTable_visit to load.",
                              dtype=str, default='coord_ra, coord_dec')


class VisitTableMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = VisitTableMeasurementTaskConfig
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
