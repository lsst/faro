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


def calculateStarGal(catalog,refCat, config):
    """Calculate star and galaxy completeness and purity metrics. 

    Parameters
    ----------
    catalog : `parquet table`
        
    refCat : `parquet table` 
    
    config:
        Important parts of config are raColumn,decColumn, refRaColumn, 
        refDecColumn and metricColumn


    Returns
    -------
    statistics : `dict`
        astrometric absolute median deviation and ancillary quantities calculated 
        from the input catalog and matched refCat. Fields are:
        - ``count``: number of sources used for calculation.
        - ``medianAbsDeviation``: `~astropy.unit.Quantity` Median absolute deviation 
            of sources in the catalog and refCat computed using metricColumn in mas
        - ``rms``: `~astropy.unit.Quantity` RMS of positions of sources in the catalog 
            and refCat computed using metricColumn in mas.
        - ``center``: scalar `~astropy.unit.Quantity` of the median separation
          of sources in the catalog and refCat computed using metricColumn in mas.
        - ``metricResid``: `~astropy.unit.Quantity` array containing the astrometric 
            residuals, in mas, using the metricColumn.
    """
    catDec = np.array(catalog[config.decColumn])
    refCatDec = np.array(refCat[config.refDecColumn])

    if (config.metricColumn == "ra") | (config.metricColumn == "total"):
        catRa = np.array(catalog[config.raColumn])
        refCatRa = np.array(refCat[config.refRaColumn])
    
    
    if config.metricColumn == "dec":
        separations = catDec-refCatDec
    elif config.metricColumn == "ra":
        # computing ra * cos(dec)
        catDec,refCatDec = np.radians([catDec,refCatDec])
        catRaCorr = catRa * np.cos(catDec)
        refCatRaCorr=refCatRa * np.cos(refCatDec)
        separations = catRaCorr - refCatRaCorr
    elif config.metricColumn == "total":
        # lon1,lat1,lon2,lat2 all angles should be in degrees
        # Uses Great-circle_distance
        # could speed up by using sep from matching step
        separations = angsep(catalog[config.raColumn],catalog[config.decColumn],
                            refCat[config.refRaColumn],refCat[config.refDecColumn])
    else: 
        return {"nomeas": np.nan * u.mas}
    # convert separations to mas
    separations = (np.array(separations) * u.deg).to(u.mas)

    return {"count":len(separations),
            "medianAbsDeviation":np.median(abs(separations)),
            "rms": np.nanstd(separations),
            "center":np.median(separations),
            "metricResid":separations,
    }