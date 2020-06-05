import astropy.units as u
from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config
from lsst.verify import Measurement

class NumSourcesTask(Task):

    ConfigClass = Config
    _DefaultName = "numSourcesTask"

    def run(self, source_catalog, metric_name):
        self.log.info(f"Counting sources in single-visit catalog")
        nSources = len(source_catalog)
        meas = Measurement("nsrcMeasVisit", nSources * u.count)
        return Struct(measurement=meas)
