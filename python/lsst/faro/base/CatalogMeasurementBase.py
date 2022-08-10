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
from astropy.table import Table, hstack
import astropy.units as u

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
from lsst.pipe.tasks.loadReferenceCatalog import LoadReferenceCatalogTask
import lsst.geom
from .BaseSubTasks import NumSourcesTask

__all__ = (
    "CatalogMeasurementBaseConnections",
    "CatalogMeasurementBaseConfig",
    "CatalogMeasurementBaseTask",
)


class CatalogMeasurementBaseConnections(
    MetricConnections, defaultTemplates={"refDataset": ""}
):

    refCat = pipeBase.connectionTypes.PrerequisiteInput(
        doc="Reference catalog",
        name="{refDataset}",
        storageClass="SimpleCatalog",
        dimensions=("skypix",),
        deferLoad=True,
        multiple=True,
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.connections.refDataset == "":
            self.prerequisiteInputs.remove("refCat")


class CatalogMeasurementBaseConfig(
    MetricConfig, pipelineConnections=CatalogMeasurementBaseConnections
):
    """Configuration for CatalogMeasurementBaseTask."""

    measure = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumSourcesTask,
        doc="Measure task",
    )

    referenceCatalogLoader = pexConfig.ConfigurableField(
        target=LoadReferenceCatalogTask, doc="Reference catalog loader",
    )

    def setDefaults(self):
        self.referenceCatalogLoader.refObjLoader.ref_dataset_name = ""
        self.referenceCatalogLoader.doApplyColorTerms = False

    def validate(self):
        super().validate()
        if (
            self.connections.refDataset
            != self.referenceCatalogLoader.refObjLoader.ref_dataset_name
        ):
            msg = "The reference datasets specified in connections and reference catalog loader must match."
            raise pexConfig.FieldValidationError(
                CatalogMeasurementBaseConfig.referenceCatalogLoader, self, msg
            )


class CatalogMeasurementBaseTask(MetricTask):
    """Base class for science performance metrics measured from source/object catalogs."""

    ConfigClass = CatalogMeasurementBaseConfig
    _DefaultName = "catalogMeasurementBaseTask"

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask("measure")

    def run(self, **kwargs):
        if 'shelveName' in self.measure.config.keys():
            if self.measure.config.shelveName:
                # Persist in-memory objects for development and testing
                self._persistMeasurementInputs(self.measure.config, self.measure.config.shelveName, **kwargs)
        return self.measure.run(self.config.connections.metric, **kwargs)

    def _getTableColumnsSelectors(self, columns, currentBands=None):
        """given a list of selectors return columns required to apply these
        selectors.
        Parameters
        ----------
        columns:  `list` [`str`]
        a list of columns required to calculate a metric. This list
        is appended with any addditional columns required for the selectorActions.

        currentBands:  `list` [`str`]
        The filter band(s) associated with the observations.

        Returns
        -------
        columnNames: `list` [`str`] the set of columns required to compute a
        metric with any addditional columns required for the selectorActions
        appended to the set.

        """
        columnNames = set(columns)
        for actionStruct in [self.config.measure.selectorActions]:
            for action in actionStruct:
                for col in action.columns(currentBands):
                    columnNames.add(col)

        return columnNames

    def _getReferenceCatalog(self, butlerQC, dataIds, refCats, filterList, epoch=None):
        """Load reference catalog in sky region of interest and optionally applies proper
        motion correction and color terms.

        Loads the `lsst.afw.table.SimpleCatalog` reference catalog, computes ra and dec
        (optionally) applying a proper motion correction. Also, color terms
        are (optionally) applied to the reference magnitudes in order to transform
        them to the data's photometric system.

        returns a refCat with both the original loaded reference catalog and
        the coorected coordinates (ra,dec) and transformed reference magnitudes
        (refMag-/refMagErr-)

        Parameters
        ----------
        butlerQC : `lsst.pipe.base.butlerQuantumContext.ButlerQuantumContext`
            Butler quantum context for a Gen3 repository.
        dataIds: interable of `lsst.daf.butler.dataId`
             An iterable object of dataIds that point to reference catalogs
             in a Gen3 repository.
        refCats : iterable of `lsst.daf.butler.DeferredDatasetHandle`
            An iterable object of dataset refs for reference catalogs in
            a Gen3 repository.
        filterList : `list` [`str`]
            List of camera physicalFilter names to apply color terms.
        epoch : `astropy.time.Time`, optional
            Epoch to which to correct proper motion and parallax
            (if available), or `None` to not apply such corrections.

        Returns
        -------
        refCat: pandas.dataframe
            a reference catalog with original columns and corrected
            coordinates (ra,dec) and reference magnitudes (refMag-/refMagErr-)
        """
        center = lsst.geom.SpherePoint(
            butlerQC.quantum.dataId.region.getBoundingCircle().getCenter()
        )
        radius = butlerQC.quantum.dataId.region.getBoundingCircle().getOpeningAngle()

        loaderTask = LoadReferenceCatalogTask(
            config=self.config.referenceCatalogLoader, dataIds=dataIds, refCats=refCats,
            name=self.config.connections.refCat
        )

        # Get catalog with proper motion and color terms applied
        refCatCorrected = loaderTask.getSkyCircleCatalog(
            center, radius, filterList, epoch=epoch
        )

        # Get unformatted catalog w/ all columns
        skyCircle = loaderTask.refObjLoader.loadSkyCircle(
            center, radius, loaderTask._referenceFilter, epoch=epoch
        )
        refCat = skyCircle.refCat

        refCatTable = Table()
        refCatTable['ra'] = refCatCorrected['ra']*u.deg
        refCatTable['dec'] = refCatCorrected['dec']*u.deg
        for n, filterName in enumerate(filterList):
            refCatTable['refMag-' + filterName] = refCatCorrected["refMag"][:, n]*u.ABmag
            refCatTable['refMagErr-' + filterName] = refCatCorrected["refMagErr"][:, n]*u.ABmag
        refCatFrame = hstack([refCatTable, refCat.asAstropy()]).to_pandas()

        return refCatFrame

    def _persistMeasurementInputs(self, config, shelveName, **kwargs):
        """Persist in-memory objects sent as inputs to metric measurement run method.

        This function is intended to be used for development and testing purposes
        as a debug tool and is NOT to be used in routine operation.

        Parameters
        ----------
        config : `lsst.pex.config.Config`
            Config to be saved.
        shelveName : `str`
            Filename for output shelve.
        """

        import shelve

        with shelve.open(shelveName, 'n') as shelf:
            shelf['config'] = config
            for key in kwargs.keys():
                shelf[key] = kwargs[key]
