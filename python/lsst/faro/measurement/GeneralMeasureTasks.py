import numpy as np
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement

__all__ = ("HistMedianTask", )


class HistMedianTask(Task):

    ConfigClass = Config
    _DefaultName = "histMedianTask"

    def run(self, measurements, agg_name, package, metric):
        self.log.info(f"Computing the {agg_name} of {package}_{metric} values")
        values = measurements[0].extras['values'].quantity
        bins = measurements[0].extras['bins'].quantity
        for m in measurements[1:]:
            values += m.extras['values'].quantity
        c = np.cumsum(values)
        idx = np.searchsorted(c, c[-1]/2.)
        # This is the bin lower bound
        lower = bins[idx]
        # This is the bin upper bound
        upper = bins[idx+1]

        # Linear interpolation of median value within the bin
        frac = (c[-1]/2. - c[idx-1])/(c[idx] - c[idx-1])
        interp = lower + (upper - lower)*frac

        return Struct(measurement=Measurement(f"metricvalue_{agg_name}_{package}_{metric}", interp))
