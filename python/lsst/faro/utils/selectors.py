# Note: analysis_drp is not yet part of the pipelines, so you need to clone it,
from lsst.pex.config import ListField, Field
from lsst.pipe.tasks.dataFrameActions import DataFrameAction
import numpy as np

__all__ = ("SNRSelector", "PerBandFlagSelector", "StarIdentifier", "GalaxyIdentifier",
           "UnknownIdentifier", "FlagSelector", "applySelectors")

def brightIsolatedStarSourceTable(config):
    #will want to put more thought into this 
    #setup SNRSelector
    config.selectorActions.SNRSelector = SNRSelector
    config.selectorActions.SNRSelector.fluxType = "psfFlux"
    config.selectorActions.SNRSelector.snrMin = 50  
    config.selectorActions.SNRSelector.bands=[""]
    
    #setup stellarSelector
    config.selectorActions.StarIdentifier = StarIdentifier
    config.selectorActions.StarIdentifier.bands=[""]
    #setup flag slectors
    config.selectorActions.FlagSelector = FlagSelector
    config.selectorActions.FlagSelector.selectWhenTrue = ["detect_isPrimary"]
    config.selectorActions.FlagSelector.selectWhenFalse = ["pixelFlags_saturated", "pixelFlags_cr", "pixelFlags_bad", "pixelFlags_edge","deblend_nChild"]
    return config


def brightIsolatedStarObjectTable(config, bands):
    #will want to put more thought into this 
    #setup SNRSelector
    config.perBandSelectorActions.SNRSelector = SNRSelector
    config.perBandSelectorActions.SNRSelector.fluxType = "psfFlux"
    config.perBandSelectorActions.SNRSelector.snrMin = 50  
    config.perBandSelectorActions.SNRSelector.bands=bands
    config.perBandSelectorActions.SNRSelector.singleSelection=True
    #setup stellarSelector
    config.perBandSelectorActions.StarIdentifier = StarIdentifier
    config.perBandSelectorActions.StarIdentifier.bands=bands
    #setup non band flag slectors
    config.selectorActions.FlagSelector = FlagSelector
    config.selectorActions.FlagSelector.selectWhenTrue = ["detect_isPrimary"]
    #setup per band flag selectors 
    config.perBandSelectorActions.PerBandFlagSelector = PerBandFlagSelector
    config.perBandSelectorActions.PerBandFlagSelector.selectWhenFalse = ["pixelFlags_saturated", "pixelFlags_cr", "pixelFlags_bad", "pixelFlags_edge"]
    config.perBandSelectorActions.PerBandFlagSelector.bands=bands
    return config


def applySelectors(catalog, selectorList,kwargs, returnMask=False):
    """Apply the selectors to narrow down the sources to use"""
    if "band" in kwargs:
        bands=kwargs["band"]
    elif "bands" in kwargs:
        bands=kwargs["bands"]
    else:
        bands=None
    mask = np.ones(len(catalog), dtype=bool)
    for selectorStruct in selectorList:
        for selector in selectorStruct:
            mask &= selector(catalog,bands=bands)
    if returnMask:
        return catalog[mask], mask
    else:
        return catalog[mask]


class SNRSelector(DataFrameAction):
    """Selects points that have snrMin < S/N < snrMax in the given flux type"""
    fluxType = Field(doc="Flux type to calculate the S/N in.",
                     dtype=str,
                     default="psfFlux")
    snrMin = Field(doc="The minimum S/N threshold to remove sources with.",
                   dtype=float,
                   default=50.0)
    snrMax = Field(doc="The maximum S/N threshold to remove sources with.",
                   dtype=float,
                   default=np.Inf)
    jointSelectionBands = ListField(doc="The maximum S/N threshold to remove sources with.",
                   dtype=str,
                   default=[""])

    def columns(self,bands=None):
        cols=[]
        if self.jointSelectionBands != [""]:
            bands=self.jointSelectionBands
        if bands is not None:
            for band in bands:
                    cols += [band+'_'+self.fluxType, band+'_'+self.fluxType+'Err']
        else:
            cols=[self.fluxType, self.fluxType+'Err']
        print(cols)
        return cols
    
    def __call__(self, df, bands=None):
        """Makes a mask of objects that have S/N between self.snrMin and
        self.snrMax in self.fluxType
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of the objects that satisfy the given
            S/N cut.
        """
        mask = np.ones(len(df), dtype=bool)
        import pdb; pdb.set_trace()
        if self.jointSelectionBands != [""]:
            bands=self.jointSelectionBands
        if bands is not None:
            for band in bands:
                mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) > self.snrMin)
                mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) < self.snrMax)
        else:
            mask &= ((df[self.fluxType] / df[self.fluxType+"Err"]) > self.snrMin)
            mask &= ((df[self.fluxType] / df[self.fluxType+"Err"]) < self.snrMax)
        return mask


class PerBandFlagSelector(DataFrameAction):
    """Flag selector for ObjectTable flags that are defined in each band
    Parameters
    ----------
    df : `pandas.core.frame.DataFrame`
    Returns
    -------
    result : `numpy.ndarray`
        A mask of the objects that satisfy the given
        flag cuts.
    Notes
    -----
    """
    selectWhenFalse = ListField(doc="Names of the flag columns to select on when False",
                                dtype=str,
                                optional=False,
                                default=[])

    selectWhenTrue = ListField(doc="Names of the flag columns to select on when True",
                               dtype=str,
                               optional=False,
                               default=[])

    bands = ListField(doc="The bands to apply the flags in",
                      dtype=str,
                      default=["g", "r", "i", "z", "y"])

    @property
    def columns(self):
        filterColumnsTrue = []
        filterColumnsFalse = []
        for band in self.bands:
            if len(band) > 0:
                filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in self.bands]
                filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in self.bands]
            else:
                filterColumnsTrue += [band+flag for flag in self.selectWhenTrue for band in self.bands]
                filterColumnsFalse += [band+flag for flag in self.selectWhenFalse for band in self.bands]

        allCols = list(filterColumnsFalse) + list(filterColumnsTrue)
        yield from allCols

    def __call__(self, df, **kwargs):
        """The flags to use for selecting sources from objectTables
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of the objects that satisfy the given
            flag cuts.
        Notes
        -----
        """

        filterColumnsTrue = []
        filterColumnsFalse = []
        for band in self.bands:
            if len(band) > 0:
                filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in self.bands]
                filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in self.bands]
            else:
                filterColumnsTrue += [band+flag for flag in self.selectWhenTrue for band in self.bands]
                filterColumnsFalse += [band+flag for flag in self.selectWhenFalse for band in self.bands]

        result = None
        for flag in filterColumnsFalse:
            selected = (df[flag].values == 0)
            if result is None:
                result = selected
            else:
                result &= selected
        for flag in filterColumnsTrue:
            selected = (df[flag].values == 1)
            if result is None:
                result = selected
            else:
                result &= selected
        return result


class StarIdentifier(DataFrameAction):
    """Identifies stars from the dataFrame"""

    bands = ListField(doc="The bands the object is to be classified as a star in.",
                      dtype=str,
                      default=["i"])

    @property
    def columns(self):
        cols = []
        for band in self.bands:
            if len(band) > 0:
                cols += [band+'_'+'extendedness']
            else:
                cols += [band+'extendedness']

        return cols

    def __call__(self, df):
        """Identifies sources classified as stars
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of objects that are classified as stars.
        """

        mask = np.ones(len(df), dtype=bool)
        for band in self.bands:
            if len(band) > 0:
                mask &= (df[band+'_'+'extendedness'] == 0.0)
            else:
                mask &= (df[band+'extendedness'] == 0.0)

        return mask


class GalaxyIdentifier(DataFrameAction):
    """Identifies galaxies from the dataFrame"""

    bands = ListField(doc="The bands the object is to be classified as a galaxy in.",
                      dtype=str,
                      default=["i"])

    @property
    def columns(self):
        cols = []
        for band in self.bands:
            if len(band) > 0:
                cols += [band+'_'+'extendedness']
            else:
                cols += [band+'extendedness']

        return cols

    def __call__(self, df):
        """Identifies sources classified as galaxies
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of objects that are classified as galaxies.
        """

        mask = np.ones(len(df), dtype=bool)
        for band in self.bands:
            if len(band) > 0:
                mask &= (df[band+'_'+'extendedness'] == 1.0)
            else:
                mask &= (df[band+'extendedness'] == 1.0)

        return mask


class UnknownIdentifier(DataFrameAction):
    """Identifies unclassified objects from the dataFrame"""

    bands = ListField(doc="The bands the object is to be classified as unknown in.",
                      dtype=str,
                      default=["i"])

    @property
    def columns(self):
        cols = []
        for band in self.bands:
            if len(band) > 0:
                cols += [band+'_'+'extendedness']
            else:
                cols += [band+'extendedness']

        return cols

    def __call__(self, df):
        """Identifies sources with unknown classification
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of objects that are unclassified.
        """

        mask = np.ones(len(df), dtype=bool)
        for band in self.bands:
            if len(band) > 0:
                mask &= (df[band+'_'+'extendedness'] == 9.0)
            else:
                mask &= (df[band+'extendedness'] == 9.0)

        return mask


# Currently everything below is copied from analysis_drp dataSelectors.py #
class FlagSelector(DataFrameAction):
    """The base flag selector to use to select valid sources for QA"""

    selectWhenFalse = ListField(doc="Names of the flag columns to select on when False",
                                dtype=str,
                                optional=False,
                                default=[])

    selectWhenTrue = ListField(doc="Names of the flag columns to select on when True",
                               dtype=str,
                               optional=False,
                               default=[])

    @property
    def columns(self):
        allCols = list(self.selectWhenFalse) + list(self.selectWhenTrue)
        yield from allCols

    def __call__(self, df, **kwargs):
        """Select on the given flags
        Parameters
        ----------
        df : `pandas.core.frame.DataFrame`
        Returns
        -------
        result : `numpy.ndarray`
            A mask of the objects that satisfy the given
            flag cuts.
        Notes
        -----
        Uses the columns in selectWhenFalse and
        selectWhenTrue to decide which columns to
        select on in each circumstance.
        """
        result = None
        for flag in self.selectWhenFalse:
            selected = (df[flag].values == 0)
            if result is None:
                result = selected
            else:
                result &= selected
        for flag in self.selectWhenTrue:
            selected = (df[flag].values == 1)
            if result is None:
                result = selected
            else:
                result &= selected
        return result
