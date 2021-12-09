
# Note: analysis_drp is not yet part of the pipelines, so you need to clone it,
from lsst.pex.config import ListField, Field
from lsst.pipe.tasks.dataFrameActions import DataFrameAction
import numpy as np

__all__ = ("SNRSelector", "PerBandFlagSelector", "StarIdentifier", "GalaxyIdentifier",
           "UnknownIdentifier", "FlagSelector")


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
    bands = ListField(doc="The bands to apply the signal to noise cut in.",
                      dtype=str,
                      default=["i"])

    @property
    def columns(self):
        cols = []
        for band in self.bands:
            cols += [band+'_'+self.fluxType, band+'_'+self.fluxType+'Err']

        return cols

    def __call__(self, df):
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
        for band in self.bands:
            mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) > self.snrMin)
            mask &= ((df[band+'_'+self.fluxType] / df[band+'_'+self.fluxType+"Err"]) < self.snrMax)

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
        # flagCols = ["PsfFlux_flag", "PixelFlags_saturatedCenter", "Extendedness_flag"]
        filterColumnsTrue = []
        filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in self.bands]
        filterColumnsFalse = []
        filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in self.bands]
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
        filterColumnsTrue += [band+'_'+flag for flag in self.selectWhenTrue for band in self.bands]
        filterColumnsFalse = []
        filterColumnsFalse += [band+'_'+flag for flag in self.selectWhenFalse for band in self.bands]

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
            cols += [band+'_'+'extendedness']

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
            mask &= (df[band+'_'+'extendedness'] == 0.0)

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
            cols += [band+'_'+'extendedness']

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
            mask &= (df[band+'_'+'extendedness'] == 1.0)

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
            cols += [band+'_'+'extendedness']

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
            mask &= (df[band+'_'+'extendedness'] == 9.0)

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
