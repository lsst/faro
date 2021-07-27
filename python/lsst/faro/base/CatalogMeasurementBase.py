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
from lsst.verify.tasks import MetricTask, MetricConfig, MetricConnections
from lsst.pipe.tasks.loadReferenceCatalog import LoadReferenceCatalogTask
import lsst.geom

from .BaseSubTasks import NumSourcesTask

__all__ = ('CatalogMeasurementBaseConnections', 'CatalogMeasurementBaseConfig',
           'CatalogMeasurementBaseTask')


class CatalogMeasurementBaseConnections(MetricConnections,
                                        defaultTemplates={'refDataset': ''}):

    refCat = pipeBase.connectionTypes.PrerequisiteInput(
        doc='Reference catalog',
        name='{refDataset}',
        storageClass='SimpleCatalog',
        dimensions=('skypix',),
        deferLoad=True,
        multiple=True
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if config.connections.refDataset == '':
            self.prerequisiteInputs.remove('refCat')


class CatalogMeasurementBaseConfig(MetricConfig,
                                   pipelineConnections=CatalogMeasurementBaseConnections):
    """Configuration for CatalogMeasurementBaseTask."""

    measure = pexConfig.ConfigurableField(
        # This task is meant to make measurements of various types.
        # The default task is, therefore, a bit of a place holder.
        # It is expected that this will be overridden in the pipeline
        # definition in most cases.
        target=NumSourcesTask,
        doc="Measure task")

    referenceCatalogLoader = pexConfig.ConfigurableField(
        target=LoadReferenceCatalogTask,
        doc="Reference catalog loader",
    )

    def setDefaults(self):
        self.referenceCatalogLoader.refObjLoader.ref_dataset_name = ''
        self.referenceCatalogLoader.doApplyColorTerms = False

    def validate(self):
        super().validate()
        if self.connections.refDataset != self.referenceCatalogLoader.refObjLoader.ref_dataset_name:
            msg = "The reference datasets specified in connections and reference catalog loader must match."
            raise pexConfig.FieldValidationError(
                CatalogMeasurementBaseConfig.referenceCatalogLoader, self, msg)


class CatalogMeasurementBaseTask(MetricTask):
    """Base class for science performance metrics measured from source/object catalogs."""

    ConfigClass = CatalogMeasurementBaseConfig
    _DefaultName = "catalogMeasurementBaseTask"

    def __init__(self, config, *args, **kwargs):
        super().__init__(*args, config=config, **kwargs)
        self.makeSubtask('measure')

    def run(self, catalog, **kwargs):
        return self.measure.run(self.config.connections.metric, catalog, **kwargs)

    def _getReferenceCatalog(self, butlerQC, dataIds, refCats, filterList, epoch=None):
        """Load reference catalog in sky region of interest.

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
        refCat : `numpy.ndarray`
            Reference catalog with color terms and proper motions
            (optionally) applied.
        """
        center = lsst.geom.SpherePoint(butlerQC.quantum.dataId.region.getBoundingCircle().getCenter())
        radius = butlerQC.quantum.dataId.region.getBoundingCircle().getOpeningAngle()

        loaderTask = LoadReferenceCatalogTask(
            config=self.config.referenceCatalogLoader,
            dataIds=dataIds,
            refCats=refCats)

        # Get catalog with proper motion and color terms applied
        refCatCorrected = loaderTask.getSkyCircleCatalog(center, radius,
                                                         filterList,
                                                         epoch=epoch)

        # Get unformatted catalog w/ all columns
        skyCircle = loaderTask.refObjLoader.loadSkyCircle(center, radius,
                                                          loaderTask._referenceFilter,
                                                          epoch=epoch)
        if not skyCircle.refCat.isContiguous():
            refCat = skyCircle.refCat.copy(deep=True)
        else:
            refCat = skyCircle.refCat

        return refCat, refCatCorrected
