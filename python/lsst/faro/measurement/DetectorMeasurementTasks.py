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

import astropy.units as u
import numpy as np

from lsst.pipe.base import Struct, Task
from lsst.pex.config import Config, Field
from lsst.verify import Measurement

__all__ = (
    "S1Config",
    "S1Task",
)


class S1Config(Config):
    doPrimary = Field(
        doc="Only count sources where detect_isPrimary is True.",
        dtype=bool,
        default=False,
    )


class S1Task(Task):
    """Compute FWHM of bright isolated sources."""

    ConfigClass = S1Config
    _DefaultName = "s1Task"

    def run(self, metricName, catalog, **kwargs):
        self.log.info("Measuring %s", metricName)
        
        def getTraceRadius(i_xx, i_yy):
            return np.sqrt((i_xx + i_yy) / 2.)

        def getFWHM(i_xx, i_yy):
            return 2 * np.sqrt(2. * np.log(2)) * getTraceRadius(i_xx, i_yy)

        snr = catalog['PsFlux'] / catalog['PsFluxErr']
        fwhm = getFWHM(catalog['Ixx'].values, catalog['Iyy'].values) # pixel

        selection = (
            catalog['detect_isPrimary'] \
            & (catalog['extendedness'] == 0.) \
            & (snr > 50.)
        )

        # plate scale to convert from pixels to arcsec
        scale = np.median([
            np.median(catalog['LocalWcs_CDMatrix_2_1'][selection]),
            np.median(catalog['LocalWcs_CDMatrix_2_1'][selection])
        ]) * u.radian.to(u.arcsec)

        if False:
            import matplotlib.pyplot as plt
            plt.ion()

            plt.figure()
            plt.xscale('log')
            plt.scatter(snr[~selection], scale * fwhm[~selection], c='0.5', s=5)
            plt.scatter(snr[selection], scale * fwhm[selection], c='blue', s=5)
            plt.show()

        s1 = scale * np.nanmedian(fwhm[selection])

        #import pdb; pdb.set_trace()

        if np.isfinite(s1):
            self.log.info("Median FWHM of bright isolated sources = %i arcsec" % s1)
        meas = Measurement("S1", s1 * u.arcsec)
        return Struct(measurement=meas)
