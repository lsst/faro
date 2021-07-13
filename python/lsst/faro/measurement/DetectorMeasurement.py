import os

import lsst.pipe.base as pipeBase
from lsst.verify.tasks import MetricConnections
import lsst.pex.config as pexConfig
import lsst.geom
from lsst.utils import getPackageDir

from lsst.faro.base.CatalogMeasurementBase import CatalogMeasurementBaseTaskConfig, CatalogMeasurementBaseTask
from lsst.pipe.tasks.loadReferenceCatalog import LoadReferenceCatalogConfig, LoadReferenceCatalogTask

__all__ = ("DetectorMeasurementTaskConfig", "DetectorMeasurementTask",
           "DetectorTableMeasurementTaskConfig", "DetectorTableMeasurementTask")


class DetectorMeasurementTaskConnections(MetricConnections,
                                         dimensions=("instrument", "visit", "detector", "band"),
                                         defaultTemplates={"photoCalibName": "calexp.photoCalib",
                                                           "externalPhotoCalibName": "fgcm",
                                                           "wcsName": "calexp.wcs",
                                                           "externalWcsName": "jointcal"}):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src"
    )
    skyWcs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="Wcs",
        name="{wcsName}"
    )
    photoCalib = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}"
    )
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
             "id for the catalog id, sorted on id for fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit wcs calibrations computed globally (with no tract information). "
             "These catalogs use the detector id for the catalog id, sorted on id for "
             "fast lookup."),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-tract, per-visit photometric calibrations.  These catalogs use the "
             "detector id for the catalog id, sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=("Per-visit photometric calibrations computed globally (with no tract "
             "information).  These catalogs use the detector id for the catalog id, "
             "sorted on id for fast lookup."),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}"
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.doApplyExternalSkyWcs:
            if config.useGlobalExternalSkyWcs:
                self.inputs.remove("externalSkyWcsTractCatalog")
            else:
                self.inputs.remove("externalSkyWcsGlobalCatalog")
        else:
            self.inputs.remove("externalSkyWcsTractCatalog")
            self.inputs.remove("externalSkyWcsGlobalCatalog")
        if config.doApplyExternalPhotoCalib:
            if config.useGlobalExternalPhotoCalib:
                self.inputs.remove("externalPhotoCalibTractCatalog")
            else:
                self.inputs.remove("externalPhotoCalibGlobalCatalog")
        else:
            self.inputs.remove("externalPhotoCalibTractCatalog")
            self.inputs.remove("externalPhotoCalibGlobalCatalog")


class DetectorMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                    pipelineConnections=DetectorMeasurementTaskConnections):
    doApplyExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the external wcs.",
                                            dtype=bool, default=False)
    useGlobalExternalSkyWcs = pexConfig.Field(doc="Whether or not to use the global external wcs.",
                                              dtype=bool, default=False)
    doApplyExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the external photoCalib.",
                                                dtype=bool, default=False)
    useGlobalExternalPhotoCalib = pexConfig.Field(doc="Whether or not to use the global external photoCalib.",
                                                  dtype=bool, default=False)


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementTaskConfig
    _DefaultName = "detectorMeasurementTask"

    def run(self, catalog, photoCalib, skyWcs):
        return self.measure.run(catalog, self.config.connections.metric)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        if self.config.doApplyExternalPhotoCalib:
            detector = inputRefs.catalog.dataId['detector']
            if self.config.useGlobalExternalPhotoCalib:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibGlobalCatalog')
            else:
                externalPhotoCalibCatalog = inputs.pop('externalPhotoCalibTractCatalog')
            row = externalPhotoCalibCatalog.find(detector)
            externalPhotoCalib = row.getPhotoCalib()
            inputs['photoCalib'] = externalPhotoCalib
        if self.config.doApplyExternalSkyWcs:
            detector = inputRefs.catalog.dataId['detector']
            if self.config.useGlobalExternalSkyWcs:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsGlobalCatalog')
            else:
                externalSkyWcsCatalog = inputs.pop('externalSkyWcsTractCatalog')
            row = externalSkyWcsCatalog.find(detector)
            externalSkyWcs = row.getWcs()
            inputs['skyWcs'] = externalSkyWcs

        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)


class DetectorTableMeasurementTaskConnections(MetricConnections,
                                              dimensions=("instrument", "visit", "detector", "band"),
                                              defaultTemplates={"refDataset": ""}):

    catalog = pipeBase.connectionTypes.Input(doc="Source catalog for visit.",
                                             dimensions=("instrument", "visit", "band"),
                                             storageClass="DataFrame",
                                             name="sourceTable_visit",
                                             deferLoad=True)

    refcat = pipeBase.connectionTypes.PrerequisiteInput(
        doc="Reference catalog.",
        name="{refDataset}",
        storageClass="SimpleCatalog",
        dimensions=("skypix",),
        deferLoad=True,
        multiple=True
    )

    measurement = pipeBase.connectionTypes.Output(doc="Per-detector measurement.",
                                                  dimensions=("instrument", "visit", "detector", "band"),
                                                  storageClass="MetricValue",
                                                  name="metricvalue_{package}_{metric}")

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.connections.refDataset == '':
            self.prerequisiteInputs.remove("refcat")


class DetectorTableMeasurementTaskConfig(CatalogMeasurementBaseTaskConfig,
                                         pipelineConnections=DetectorTableMeasurementTaskConnections):
    columns = pexConfig.Field(doc="Columns from sourceTable_visit to load.",
                              dtype=str, default='coord_ra, coord_dec, detector')
    refDataset = pexConfig.Field(doc="Reference dataset to use.",
                                 dtype=str, default='')


class DetectorTableMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorTableMeasurementTaskConfig
    _DefaultName = "detectorTableMeasurementTask"

    def run(self, catalog, dataIds, refcat, refcatCalib):
        return self.measure.run(catalog, self.config.connections.metric, dataIds)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        columns = [_.strip() for _ in self.config.columns.split(',')]
        catalog = inputs['catalog'].get(parameters={'columns': columns})
        selection = (catalog['detector'] == butlerQC.quantum.dataId['detector'])
        inputs['catalog'] = catalog[selection]
        inputs['dataIds'] = [butlerQC.registry.expandDataId(inputRefs.catalog.datasetRef.dataId)]

        if self.config.connections.refDataset != '':
            config = LoadReferenceCatalogConfig()
            config.refObjLoader.ref_dataset_name = self.config.connections.refDataset

            # Probably these defaults should be set elsewhere in a common utility
            if self.config.connections.refDataset == 'gaia_dr2_20200414':
                # Apply proper motions for Gaia catalog
                config.refObjLoader.requireProperMotion = True
                config.refObjLoader.anyFilterMapsToThis = 'phot_g_mean'
                config.doApplyColorTerms = False
            elif self.config.connections.refDataset == 'ps1_pv3_3pi_20170110':
                # Apply color terms for PS1 catalog
                config.refObjLoader.load(os.path.join(getPackageDir('obs_subaru'),
                                                      'config',
                                                      'filterMap.py'))
                config.colorterms.load(os.path.join(getPackageDir('obs_subaru'),
                                                    'config',
                                                    'colorterms.py'))

            # Directly concatenate the reference catalogs
            # This approach does NOT match rows with LoadReferenceCatalogTask
            # refCat = inputs['refcat'][0].get()
            # for handle in inputs['refcat'][1:]:
            #     refCat.extend(handle.get())
            # if not refCat.isContiguous():
            #     refCat = refCat.copy(deep=True)
            # inputs['refcat'] = refCat

            center = lsst.geom.SpherePoint(butlerQC.quantum.dataId.region.getBoundingCircle().getCenter())
            radius = butlerQC.quantum.dataId.region.getBoundingCircle().getOpeningAngle()

            epoch = butlerQC.quantum.dataId.records['visit'].timespan.begin

            loaderTask = LoadReferenceCatalogTask(
                config=config,
                dataIds=[ref.datasetRef.dataId
                         for ref in inputRefs.refcat],
                refCats=inputs.pop('refcat'))

            # Catalog with color terms applied
            inputs['refcatCalib'] = loaderTask.getSkyCircleCatalog(
                center,
                radius,
                [butlerQC.quantum.dataId.records['physical_filter'].name],
                epoch=epoch)

            # Next get unformatted catalog
            skyCircle = loaderTask.refObjLoader.loadSkyCircle(center, radius,
                                                              loaderTask._referenceFilter,
                                                              epoch=epoch)
            if not skyCircle.refCat.isContiguous():
                inputs['refcat'] = skyCircle.refCat.copy(deep=True)
            else:
                inputs['refcat'] = skyCircle.refCat

            # Plotting is only for testing during development
            # import matplotlib.pyplot as plt
            # import numpy as np
            # plt.figure()
            # plt.scatter(inputs['catalog']['coord_ra'],
            #             inputs['catalog']['coord_dec'], marker='+')
            # plt.scatter(np.degrees(refCat['coord_ra']),
            #             np.degrees(refCat['coord_dec']), marker='x')
            # plt.scatter(refCatCalib['ra'], refCatCalib['dec'], marker='o')
            # plt.show()
            # plt.savefig('test.png')
            # input('WAIT')

        # import pdb; pdb.set_trace()

        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug("Skipping measurement of {!r} on {} "
                           "as not applicable.", self, inputRefs)
