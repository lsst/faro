import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config, Field
from lsst.verify import Measurement

from lsst.faro.utils.matcher import mergeCatalogs

__all__ = ('NumSourcesTask', 'NumSourcesMergeTask',
           'NumpySummaryTaskConfig', 'NumpySummaryTask')


class NumSourcesTaskConfig(Config):
    doPrimary = Field(doc="Only count sources where detect_isPrimary is True.",
                      dtype=bool, default=False)


class NumSourcesTask(Task):
    """Simple default task count the number of sources/objects in catalog."""

    ConfigClass = NumSourcesTaskConfig
    _DefaultName = "numSourcesTask"

    def run(self, metricName, catalog, **kwargs):
        self.log.info("Measuring %s", metricName)
        if self.config.doPrimary:
            nSources = np.sum(catalog['detect_isPrimary'] is True)
        else:
            nSources = len(catalog)
        self.log.info("Number of sources (nSources) = %i" % nSources)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)


class NumSourcesMergeTask(Task):

    ConfigClass = Config
    _DefaultName = "numSourcesMergeTask"

    def run(self, metricName, catalogs, photoCalibs, astromCalibs, **kwargs):
        self.log.info("Measuring %s", metricName)
        catalog = mergeCatalogs(catalogs, photoCalibs, astromCalibs)
        nSources = len(catalog)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)


class NumpySummaryTaskConfig(Config):
    summary = Field(dtype=str, default="median",
                    doc="Aggregation to use for summary metrics")


class NumpySummaryTask(Task):

    ConfigClass = NumpySummaryTaskConfig
    _DefaultName = "numpySummaryTask"

    def run(self, measurements, agg_name, package, metric):
        agg = agg_name.lower()
        if agg == "summary":
            agg = self.config.summary
        self.log.info("Computing the %s of %s_%s values", agg, package, metric)

        if len(measurements) == 0:
            self.log.info('Received zero length measurements list.  Returning NaN.')
            # In the case of an empty list, there is nothing we can do other than
            # to return a NaN
            value = u.Quantity(np.nan)
        else:
            unit = measurements[0].quantity.unit
            value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements
                                                 if np.isfinite(x.quantity)]))
            # Make sure return has same unit as inputs
            # In some cases numpy can return a NaN and the unit gets dropped
            value = value.value*unit
        return Struct(measurement=Measurement(f"metricvalue_{agg_name.lower()}_{package}_{metric}", value))
