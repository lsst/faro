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

        if len(measurements) == 0:
            self.log.info(f'Recieved zero length measurments list.  Returning NaN.')
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


class HistModeTask(Task):

    ConfigClass = Config
    _DefaultName = "histModeTask"

    def run(self, measurements, agg_name, package, metric):
        self.log.info(f"Computing the {agg_name} of {package}_{metric} values")
        values = measurements[0].extras['values'].quantity
        bins = measurements[0].extras['bins'].quantity
        for m in measurements[1:]:
            values += m.extras['values'].quantity
        idx = np.argmax(values)
        value = (bins[idx] + bins[idx+1])/2.

        return Struct(measurement=Measurement(f"metricvalue_{agg_name}_{package}_{metric}", value))
