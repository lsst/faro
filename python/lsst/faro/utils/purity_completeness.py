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
import numpy as np
import astropy.units as u

def get_nearest_points(centers,points,val):
    aboveIndex=np.where(points==points[points > val][np.argmin(np.abs(points[points > val]-val))])[0]
    belowIndex=aboveIndex+1
    print(aboveIndex)
    if np.abs(points[belowIndex]-val)==np.min(np.abs(points[points < val]-val)):
        return aboveIndex,belowIndex
    else: 
        return
def get_mag_at_val(centers,points,val):
    try: 
        aboveIndex,belowIndex=get_nearest_points(centers,points,val)
    except:
        return np.nan * u.ABmag
    slope = (points[aboveIndex]-points[belowIndex])/(centers[aboveIndex]-centers[belowIndex])
    print(slope)
    magval= (val-points[aboveIndex])/slope+centers[aboveIndex]
    return magval * u.ABmag

def calculateStarGal(catalog,refCat, config, metricName):
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
    catType = np.array(catalog[config.typeColumn])
    refCatType = np.array(refCat[config.refTypeColumn])
    catMag = (catalog[config.fluxColumn].values*u.nJy).to(u.ABmag)
    magBins = np.arange(20,28+0.4,0.4)
    magBinCenters = (magBins[:-1]+magBins[1:])/2
    trueStars=(refCatType==0)
    trueGals=(refCatType==1)
    kwargs = dict(true_pos=trueStars,true_neg=trueGals,mag=catMag,bins=magBins)

    #smatrix_ls = odict([[i,confusion_matrix(matchSrcCat["extendedness"]==0, matchSrcCat["extendedness"]!=0,**kwargs)] for i in range(1)])
    if metricName == "StarCompleteness":
        posFlag=0
        negFlag=1
        conDict={"class_pos":(catType == posFlag),
                 "class_neg":(catType == negFlag),
                 "true_pos":(refCatType == posFlag),
                 "true_neg":(refCatType == negFlag)}
        res=confusion_matrix(**conDict)
        magdepth=get_mag_at_val(magBinCenters,res["TPR"],0.5)
        return magdepth
    else: 
        return {"nomeas": np.nan * u.mas}
    # convert separations to mas
    #separations = (np.array(separations) * u.deg).to(u.mas)
    
    #return res
    # {"count":len(separations),
    #         "medianAbsDeviation":np.median(abs(separations)),
    #         "rms": np.nanstd(separations),
    #         "center":np.median(separations),
    #         "metricResid":separations,
    # }
# def completenessPurity(class_pos,class_neg,true_pos,true_neg,mag,bins,metric):

#     if metric == "completeness":
#         # True Positive (TP)
#         TP,junk = np.histogram(mag,bins,weights=(class_pos&true_pos).astype(float))
#          # True Positive Rate (TPR) aka Sensitivity aka Efficiency aka Completeness
#         TPR = TP/true_npos
#         ret = TPR
#     elif metric == "purity":

#     else:
#         print("bad metric")
#         return
#     return ret
def confusion_matrix(class_pos,class_neg,true_pos,true_neg,mag=None,bins=None):
    """
    Adapted from desqr code by Alex Drlica-Wagner 
    https://github.com/kadrlica/desqr/
    Define a confusion matrix for statistical classification. For more
    information on this object, see the wikipedia page here:
    https://en.wikipedia.org/wiki/Confusion_matrix
    Parameters:
    -----------
    class_pos : Boolean array of objects classified as positive
    class_neg : Boolean array of objects classified as negative
    true_pos  : Boolean array of objects that are true positive
    true_neg  : Boolean array of objects that are true negative
    mag       : A parameter over which the calculate statistics
    bins      : Bins of mag to use to calculate statistics
    Returns:
    --------
    matrix    : A confusion matrix dict (see reference for keys)
    # See https://en.wikipedia.org/wiki/Sensitivity_and_specificity
    # and https://en.wikipedia.org/wiki/Confusion_matrix
    """
    

    scalar=False
    if mag is None or np.isscalar(class_pos):
        scalar = True
    if mag is None:
        mag = np.ones_like(class_pos,dtype=float)
    if bins is None:
        bins = [np.min(mag),np.max(mag)]

    # True Positive (TP)
    TP,junk = np.histogram(mag,bins,weights=(class_pos&true_pos).astype(float))
    # True Negative (TN)
    TN,junk = np.histogram(mag,bins,weights=(class_neg&true_neg).astype(float))
    # False Positive (FP)
    FP,junk = np.histogram(mag,bins,weights=(class_pos&true_neg).astype(float))
    # False Negative (FN)
    FN,junk = np.histogram(mag,bins,weights=(class_neg&true_pos).astype(float))

    # These are generally called:
    # P = true_npos, N = true_nneg
    class_npos,j = np.histogram(mag,bins,weights=class_pos.astype(float))
    class_nneg,j = np.histogram(mag,bins,weights=class_neg.astype(float))
    true_npos,j  = np.histogram(mag,bins,weights=true_pos.astype(float) )
    true_nneg,j  = np.histogram(mag,bins,weights=true_neg.astype(float) )

    # True Positive Rate (TPR) aka Sensitivity aka Efficiency aka Completeness
    TPR = TP/true_npos
    # False negative rate (FNR) aka Miss rate
    FNR = FN/true_npos
    # True negative rate (TNR) aka Specificity (SPC)
    TNR = TN/true_nneg
    # False positive rate (FPR) aka Fall-out
    FPR = FP/true_nneg

    # False Discovery Rate (FDR) aka contamination
    FDR = FP/class_npos
    # Positive predictive value (PPV) aka precision aka purity
    PPV = TP/class_npos
    # False omission rate (FOR)
    FOR = FN/class_nneg
    # Negative predictive value (NPV)
    NPV = TN/class_nneg

    #Accuracy (ACC)
    ACC = (TP + TN)/(true_npos + true_nneg)
    # Prevalence (PRV)
    PRV = class_npos/(true_npos + true_nneg)

    # Positive likelihood ratio (PLR)
    PLR = TPR/FPR
    # Negative likelihood ration (NLR)
    NLR = FNR/TNR

    # Return a dictionary of all values
    ret = dict(TP=TP,TN=TN,FP=FP,FN=FN)
    ret.update(TPR=TPR, FNR=FNR, TNR=TNR, FPR=FPR)
    ret.update(PPV=PPV, FDR=FDR, FOR=FOR, NPV=NPV)
    ret.update(ACC=ACC,PRV=PRV,PLR=PLR,NLR=NLR)
    ret.update(true_npos=true_npos,true_nneg=true_nneg)
    ret.update(P=true_npos,N=true_nneg)
    ret.update(class_npos=class_npos,class_nneg=class_nneg)

    # Also define lowercase aliases
    for key,val in list(ret.items()):
        ret[key.lower()] = val

    if scalar:
        for key,val in ret.items():
            ret[key] = np.asscalar(val)

    return ret
