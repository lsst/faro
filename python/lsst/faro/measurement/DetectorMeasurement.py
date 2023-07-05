# This file is part of faro.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig

from lsst.faro.base.CatalogMeasurementBase import (
    CatalogMeasurementBaseConnections,
    CatalogMeasurementBaseConfig,
    CatalogMeasurementBaseTask,
)

__all__ = ("DetectorMeasurementConfig", "DetectorMeasurementTask")


class DetectorMeasurementConnections(
    CatalogMeasurementBaseConnections,
    dimensions=("instrument", "visit", "detector", "band"),
    # TODO: remove deprecated templates on DM-39854.
    defaultTemplates={
        "photoCalibName": "calexp.photoCalib",
        "externalPhotoCalibName": "fgcm",
        "wcsName": "calexp.wcs",
        "externalWcsName": "gbdesAstrometricFit",
    },
    deprecatedTemplates={
        "photoCalibName": "Deprecated in favor of visitSummary; will be removed after v27.",
        "externalPhotoCalibName": "Deprecated in favor of visitSummary; will be removed after v27.",
        "wcsName": "Deprecated in favor of visitSummary; will be removed after v27.",
        "externalWcsName": "Deprecated in favor of visitSummary; will be removed after v27.",
    },
):

    catalog = pipeBase.connectionTypes.Input(
        doc="Source catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src",
    )
    visitSummary = pipeBase.connectionTypes.Input(
        doc="Exposure catalog with WCS and PhotoCalib this detector+visit combination.",
        dimensions=("instrument", "visit"),
        storageClass="ExposureCatalog",
        name="finalVisitSummary",
    )
    skyWcs = pipeBase.connectionTypes.Input(
        doc="WCS for the catalog.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="Wcs",
        name="{wcsName}",
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary and already ignored; will be removed after v27."
    )
    photoCalib = pipeBase.connectionTypes.Input(
        doc="Photometric calibration object.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="PhotoCalib",
        name="{photoCalibName}",
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary and already ignored; will be removed after v27."
    )
    externalSkyWcsTractCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-tract, per-visit wcs calibrations.  These catalogs use the detector "
            "id for the catalog id, sorted on id for fast lookup."
        ),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary; will be removed after v27."
    )
    externalSkyWcsGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-visit wcs calibrations computed globally (with no tract information). "
            "These catalogs use the detector id for the catalog id, sorted on id for "
            "fast lookup."
        ),
        name="{externalWcsName}SkyWcsCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary; will be removed after v27."
    )
    externalPhotoCalibTractCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-tract, per-visit photometric calibrations.  These catalogs use the "
            "detector id for the catalog id, sorted on id for fast lookup."
        ),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit", "tract"),
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary; will be removed after v27."
    )
    externalPhotoCalibGlobalCatalog = pipeBase.connectionTypes.Input(
        doc=(
            "Per-visit photometric calibrations computed globally (with no tract "
            "information).  These catalogs use the detector id for the catalog id, "
            "sorted on id for fast lookup."
        ),
        name="{externalPhotoCalibName}PhotoCalibCatalog",
        storageClass="ExposureCatalog",
        dimensions=("instrument", "visit"),
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of visitSummary; will be removed after v27."
    )
    measurement = pipeBase.connectionTypes.Output(
        doc="Per-detector measurement.",
        dimensions=("instrument", "visit", "detector", "band"),
        storageClass="MetricValue",
        name="metricvalue_{package}_{metric}",
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        # TODO: remove references to deprecates things after DM-39854 (may
        # allow the __init__ override to go away entirely).
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
        del self.skyWcs
        del self.photoCalib


class DetectorMeasurementConfig(
    CatalogMeasurementBaseConfig, pipelineConnections=DetectorMeasurementConnections
):
    doApplyExternalSkyWcs = pexConfig.Field(
        doc="Whether or not to use the external wcs.", dtype=bool, default=False,
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of the visitSummary connection; will be removed after v27."
    )
    useGlobalExternalSkyWcs = pexConfig.Field(
        doc="Whether or not to use the global external wcs.", dtype=bool, default=False,
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of the visitSummary connection; will be removed after v27."
    )
    doApplyExternalPhotoCalib = pexConfig.Field(
        doc="Whether or not to use the external photoCalib.", dtype=bool, default=False,
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of the visitSummary connection; will be removed after v27."
    )
    useGlobalExternalPhotoCalib = pexConfig.Field(
        doc="Whether or not to use the global external photoCalib.",
        dtype=bool,
        default=False,
        # TODO: remove on DM-39854.
        deprecated="Deprecated in favor of the visitSummary connection; will be removed after v27."
    )


class DetectorMeasurementTask(CatalogMeasurementBaseTask):
    ConfigClass = DetectorMeasurementConfig
    _DefaultName = "detectorMeasurementTask"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        visitSummary = inputs.pop("visitSummary")
        detector = inputRefs.catalog.dataId["detector"]
        row = visitSummary.find(detector)
        inputs["photoCalib"] = row.getPhotoCalib()
        inputs["skyWcs"] = row.getSkyWcs()
        # TODO: significant simplification should be possible here on DM-39854.
        if self.config.doApplyExternalPhotoCalib:
            if self.config.useGlobalExternalPhotoCalib:
                externalPhotoCalibCatalog = inputs.pop(
                    "externalPhotoCalibGlobalCatalog"
                )
            else:
                externalPhotoCalibCatalog = inputs.pop("externalPhotoCalibTractCatalog")
            row = externalPhotoCalibCatalog.find(detector)
            externalPhotoCalib = None if row is None else row.getPhotoCalib()
            inputs["photoCalib"] = externalPhotoCalib
        if self.config.doApplyExternalSkyWcs:
            if self.config.useGlobalExternalSkyWcs:
                externalSkyWcsCatalog = inputs.pop("externalSkyWcsGlobalCatalog")
            else:
                externalSkyWcsCatalog = inputs.pop("externalSkyWcsTractCatalog")
            row = externalSkyWcsCatalog.find(detector)
            externalSkyWcs = None if row is None else row.getWcs()
            inputs["skyWcs"] = externalSkyWcs

        outputs = self.run(**inputs)
        if outputs.measurement is not None:
            butlerQC.put(outputs, outputRefs)
        else:
            self.log.debug(
                "Skipping measurement of {!r} on {} " "as not applicable.",
                self,
                inputRefs,
            )
