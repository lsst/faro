import astropy.units as u
import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config, Field
from lsst.verify import Measurement


class NumSourcesTask(Task):

    ConfigClass = Config
    _DefaultName = "numSourcesTask"

    def run(self, catalog, metric_name):
        self.log.info(f"Measuring {metric_name}")
        nSources = len(catalog)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)


class NumpyAggTaskConfig(Config):
    summary = Field(dtype=str, default="median",
                              doc="Aggregation to use for summary metrics")

class NumpyAggTask(Task):

    ConfigClass = NumpyAggTaskConfig
    _DefaultName = "numpyAggTask"

    def run(self, measurements, agg_name, package, metric):
        agg = agg_name.lower()
        if agg == "summary":
            agg = self.config.summary
        self.log.info(f"Computing the {agg} of {package}_{metric} values")

        value = getattr(np, agg)(u.Quantity([x.quantity for x in measurements if np.isfinite(x.quantity)]))
        return Struct(measurement=Measurement(f"metricvalue_{agg_name.lower()}_{package}_{metric}", value))
