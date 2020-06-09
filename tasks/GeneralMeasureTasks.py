import astropy.units as u
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement


class NumSourcesTask(Task):

    ConfigClass = Config
    _DefaultName = "numSourcesTask"

    def run(self, catalog, metric_name):
        self.log.info(f"Measuring {metric_name}")
        nSources = len(catalog)
        meas = Measurement("nsrcMeas", nSources * u.count)
        return Struct(measurement=meas)