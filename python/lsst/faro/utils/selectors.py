# Note: analysis_drp is not yet part of the pipelines, so you need to clone it,
from lsst.pex.config import ListField, Field, ChoiceField
from lsst.pipe.tasks.dataFrameActions import DataFrameAction
import numpy as np

__all__ = ("FlagSelector", "GalaxyIdentifier", "PerBandFlagSelector", "SNRSelector",  "StarIdentifier", 
           "UnknownIdentifier", "applySelectors", "brightIsolatedStarSourceTable", "brightIsolatedStarObjectTable")

class FlagSelector(DataFrameAction):
    """The base flag selector to use to select valid sources shoud not have an associated band"""

    selectWhenFalse = ListField(doc="Names of the flag columns to select on when False",
                                dtype=str,
                                optional=False,
                                default=[])

    selectWhenTrue = ListField(doc="Names of the flag columns to select on when True",
                               dtype=str,
                               optional=False,
                               default=[])

    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])

    def columns(self,currentBands=None):
        allCols = list(self.selectWhenFalse) + list(self.selectWhenTrue)
        return allCols

    def __call__(self, df, currentBands=None):
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
        mask = np.ones(len(df), dtype=bool)

        for flag in self.selectWhenFalse:
            mask &= (df[flag].values == 0)

        for flag in self.selectWhenTrue:
            mask &= (df[flag].values == 1)
            
        return mask

class GalaxyIdentifier(DataFrameAction):
    """Identifies galaxies from the dataFrame"""

    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])
    
    def columns(self,currentBands=None):
        allCols=[]
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        else:
            bands=currentBands

        if bands is not None:
            for band in bands:
                    allCols += [band+'_'+'extendedness']
        else:
            allCols= ['extendedness']
        return allCols

    def __call__(self, df, currentBands=None):
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

        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        elif self.selectorBandType == "currentBands":
            bands=currentBands

        if bands is not None:
            for band in bands:
                mask &= (df[band+'_'+'extendedness'] == 1.0)
        else:
            mask &= (df['extendedness'] == 1.0)

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

    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])

    def columns(self,currentBands=None):
        filterColumnsTrue = []
        filterColumnsFalse = []
        allCols=[]
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        else:
            bands=currentBands
        if bands is not None:
            for band in bands:
                    filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in bands]
                    filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in bands]
        else:
            filterColumnsTrue += [band+flag for flag in self.selectWhenTrue for band in self.bands]
            filterColumnsFalse += [band+flag for flag in self.selectWhenFalse for band in self.bands]
        allCols = list(filterColumnsFalse) + list(filterColumnsTrue)
        return allCols

    def __call__(self, df, currentBands=None):
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
        mask = np.ones(len(df), dtype=bool)

        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        elif self.selectorBandType == "currentBands":
            bands=currentBands

        if bands is not None:
            for band in bands:
                filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in bands]
                filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in bands]
        else:
            filterColumnsTrue += [flag for flag in self.selectWhenTrue]
            filterColumnsFalse += [flag for flag in self.selectWhenFalse]

        for flag in filterColumnsFalse:
            mask &= (df[flag].values == 0)
        for flag in filterColumnsTrue:
            mask &= (df[flag].values == 1)
        return mask


class SNRSelector(DataFrameAction):
    """SNR Selector for sources from a SourceTable or ObjectTable
    Selects soruces that have snrMin < S/N < snrMax in the given flux type
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
    For input of type:
     - SourceTable: jointSelectionBands=[''], bands=None
     - ObjectTable (snr cut in current band only): jointSelectionBands=[''], bands=currentband
     - ObjectTable (snr cut over a set of bands): jointSelectionBands=['set', 'of', 'bands'], bands=None
     - ObjectTable (snr cut over a single band no matter the current band): jointSelectionBands=['band'], bands=None
     """
    fluxType = Field(doc="Flux type to calculate the S/N in.",
                     dtype=str,
                     default="psfFlux")
    snrMin = Field(doc="The minimum S/N threshold to remove sources with.",
                   dtype=float,
                   default=50.0)
    snrMax = Field(doc="The maximum S/N threshold to remove sources with.",
                   dtype=float,
                   default=np.Inf)
    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])
    # want to set an error if staticBandSet, and self.staticBandSet=[""]
    def columns(self,currentBands=None):
        allCols=[]
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        else:
            bands=currentBands

        if bands is not None:
            for band in bands:
                    allCols += [band+'_'+self.fluxType, band+'_'+self.fluxType+'Err']
        else:
            allCols=[self.fluxType, self.fluxType+'Err']
        return allCols
    
    def __call__(self, df, currentBands=None):
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
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        elif self.selectorBandType == "currentBands":
            bands=currentBands
        if bands is not None:
            for band in bands:
                mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) > self.snrMin)
                mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) < self.snrMax)
        else:
            mask &= ((df[self.fluxType] / df[self.fluxType+"Err"]) > self.snrMin)
            mask &= ((df[self.fluxType] / df[self.fluxType+"Err"]) < self.snrMax)
        return mask





class StarIdentifier(DataFrameAction):
    """Identifies stars from the dataFrame"""

    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])
    
    def columns(self,currentBands=None):
        allCols=[]
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        else:
            bands=currentBands

        if bands is not None:
            for band in bands:
                    allCols += [band+'_'+'extendedness']
        else:
            allCols= ['extendedness']
        return allCols

    def __call__(self, df, currentBands=None):
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

        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        elif self.selectorBandType == "currentBands":
            bands=currentBands

        if bands is not None:
            for band in bands:
                mask &= (df[band+'_'+'extendedness'] == 0.0)
        else:
            mask &= (df['extendedness'] == 0.0)

        return mask


class UnknownIdentifier(DataFrameAction):
    """Identifies unclassified objects from the dataFrame"""

    selectorBandType=ChoiceField(doc="Type of selection to do options are current band or static selection",
                    dtype=str,
                    allowed={"currentBands":"use the band of the current quanta for objectTable or None for sourceTable", 
                            "staticBandSet":"use the bands listed in self.staticBandSet"
                            },
                    default="currentBands",)
    staticBandSet = ListField(doc="""set of bands that selection should be 
                    applied over. If changed from the default this overrides 
                    the band argument in the columns/call method.""",
                    dtype=str,
                    default=[""])
    
    def columns(self, currentBands=None):
        allCols=[]
        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        else:
            bands=currentBands

        if bands is not None:
            for band in bands:
                    allCols += [band+'_'+'extendedness']
        else:
            allCols= ['extendedness']
        return allCols

    def __call__(self, df, currentBands=None):
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

        if self.selectorBandType == "staticBandSet":
            bands=self.staticBandSet
        elif self.selectorBandType == "currentBands":
            bands=currentBands

        if bands is not None:
            for band in bands:
                mask &= (df[band+'_'+'extendedness'] == 9.0)
        else:
            mask &= (df['extendedness'] == 9.0)

        return mask


def applySelectors(catalog, selectorList,currentBands=None, returnMask=False):
    """Apply the selectors to narrow down the sources to use"""
    mask = np.ones(len(catalog), dtype=bool)
    for selector in selectorList:
        mask &= selector(catalog,currentBands=currentBands)
    if returnMask:
        return catalog[mask], mask
    else:
        return catalog[mask]


def brightIsolatedStarSourceTable(config):
    #will want to put more thought into this 
    #setup SNRSelector
    config.selectorActions.SNRSelector = SNRSelector
    config.selectorActions.SNRSelector.fluxType = "psfFlux"
    config.selectorActions.SNRSelector.snrMin = 50  
    config.selectorActions.SNRSelector.selectorBandType="currentBands"
    #setup stellarSelector
    config.selectorActions.StarIdentifier = StarIdentifier
    config.selectorActions.StarIdentifier.selectorBandType="currentBands"
    #setup flag slectors
    config.selectorActions.FlagSelector = FlagSelector
    config.selectorActions.FlagSelector.selectWhenTrue = ["detect_isPrimary"]
    config.selectorActions.FlagSelector.selectWhenFalse = ["pixelFlags_saturated", "pixelFlags_cr", "pixelFlags_bad", "pixelFlags_edge","deblend_nChild"]
    config.selectorActions.FlagSelector.selectorBandType="currentBands"
    return config

def brightIsolatedStarObjectTable(config):
    #will want to put more thought into this 
    #setup SNRSelector
    config.selectorActions.SNRSelector = SNRSelector
    config.selectorActions.SNRSelector.fluxType = "psfFlux"
    config.selectorActions.SNRSelector.snrMin = 50  
    config.selectorActions.SNRSelector.selectorBandType="currentBands"
    #setup stellarSelector
    config.selectorActions.StarIdentifier = StarIdentifier
    config.selectorActions.StarIdentifier.selectorBandType="currentBands"
    #setup non band flag slectors
    config.selectorActions.FlagSelector = FlagSelector
    config.selectorActions.FlagSelector.selectWhenTrue = ["detect_isPrimary"]
    config.selectorActions.FlagSelector.selectorBandType="currentBands"
    #setup per band flag selectors 
    config.selectorActions.PerBandFlagSelector = PerBandFlagSelector
    config.selectorActions.PerBandFlagSelector.selectWhenFalse = ["pixelFlags_saturated", "pixelFlags_cr", "pixelFlags_bad", "pixelFlags_edge"]
    config.selectorActions.PerBandFlagSelector.selectorBandType="currentBands"
    return config
