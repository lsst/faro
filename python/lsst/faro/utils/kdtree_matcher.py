# This file is part of faro.
# This file is an excerpt of https://github.com/DarkEnergySurvey/ugali/utils/projector
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

# Based on Calabretta & Greisen 2002, A&A, 357, 1077-1122
# http://adsabs.harvard.edu/abs/2002A%26A...395.1077C

# 

import numpy as np 

class SphericalRotator:
    """
    Base class for rotating points on a sphere.
    The input is a fiducial point (deg) which becomes (0, 0) in rotated coordinates.
    """

    def __init__(self, lon_ref, lat_ref, zenithal=False):
        self.setReference(lon_ref, lat_ref, zenithal)

    def setReference(self, lon_ref, lat_ref, zenithal=False):

        if zenithal:
            phi = (np.pi / 2.) + np.radians(lon_ref)
            theta = (np.pi / 2.) - np.radians(lat_ref)
            psi = 0.
        if not zenithal:
            phi = (-np.pi / 2.) + np.radians(lon_ref)
            theta = np.radians(lat_ref)
            # psi = 90 corresponds to (0, 0)
            # psi = -90 corresponds to (180, 0)
            psi = np.radians(90.)

        cos_psi,sin_psi = np.cos(psi),np.sin(psi)
        cos_phi,sin_phi = np.cos(phi),np.sin(phi)
        cos_theta,sin_theta = np.cos(theta),np.sin(theta)

        self.rotation_matrix = np.array([
            [cos_psi * cos_phi - cos_theta * sin_phi * sin_psi,
             cos_psi * sin_phi + cos_theta * cos_phi * sin_psi,
             sin_psi * sin_theta],
            [-sin_psi * cos_phi - cos_theta * sin_phi * cos_psi,
             -sin_psi * sin_phi + cos_theta * cos_phi * cos_psi,
             cos_psi * sin_theta],
            [sin_theta * sin_phi,
             -sin_theta * cos_phi,
             cos_theta]
        ])
        
        self.inverted_rotation_matrix = np.linalg.inv(self.rotation_matrix)

    def cartesian(self,lon,lat):
        lon = np.radians(lon)
        lat = np.radians(lat) 
        
        x = np.cos(lat) * np.cos(lon)
        y = np.cos(lat) * np.sin(lon)
        z =  np.sin(lat)
        return np.array([x,y,z])
        

    def rotate(self, lon, lat, invert=False):
        vec = self.cartesian(lon,lat)

        if invert:
            vec_prime = np.dot(np.array(self.inverted_rotation_matrix), vec)
        else:        
            vec_prime = np.dot(np.array(self.rotation_matrix), vec)

        lon_prime = np.arctan2(vec_prime[1], vec_prime[0])
        lat_prime = np.arcsin(vec_prime[2])

        return (np.degrees(lon_prime) % 360.), np.degrees(lat_prime)

class Projector:
    """
    Class for performing two-dimensional map projections from the celestial sphere.
    """

    def __init__(self, lon_ref, lat_ref, proj_type = 'ait'):
        self.lon_ref = lon_ref
        self.lat_ref = lat_ref
        self.proj_type = proj_type

        if proj_type.lower() == 'ait':
            self.rotator = SphericalRotator(lon_ref, lat_ref, zenithal=False)
            self.sphere_to_image_func = aitoffSphereToImage
            self.image_to_sphere_func = aitoffImageToSphere
        elif proj_type.lower() == 'tan':
            self.rotator = SphericalRotator(lon_ref, lat_ref, zenithal=True)
            self.sphere_to_image_func = gnomonicSphereToImage
            self.image_to_sphere_func = gnomonicImageToSphere
        elif proj_type.lower() == 'car':
            def rotate(lon,lat,invert=False):
                if invert:
                    return lon + np.array([lon_ref]), lat + np.array([lat_ref])
                else:
                    return lon - np.array([lon_ref]), lat - np.array([lat_ref])
            self.rotator = SphericalRotator(lon_ref, lat_ref, zenithal=False)
            # Monkey patch the rotate function
            self.rotator.rotate = rotate 
            self.sphere_to_image_func = cartesianSphereToImage
            self.image_to_sphere_func = cartesianImageToSphere
        else:
            logger.warn('%s not recognized'%(proj_type))

    def sphereToImage(self, lon, lat):
        scalar = np.isscalar(lon)
        
        lon, lat = np.asarray(lon), np.asarray(lat)
        lon_rotated, lat_rotated = self.rotator.rotate(lon.flat, lat.flat)
        x, y = self.sphere_to_image_func(lon_rotated, lat_rotated)

        if scalar: return np.asscalar(x), np.asscalar(y)
        else:      return x.reshape(lon.shape), y.reshape(lat.shape)

    sphere2image = sphereToImage
        
    def imageToSphere(self, x, y):
        scalar = np.isscalar(x)

        x, y = np.asarray(x), np.asarray(y)
        lon_rotated, lat_rotated = self.image_to_sphere_func(x.flat, y.flat)
        lon, lat = self.rotator.rotate(lon_rotated, lat_rotated, invert = True)

        if scalar: return np.asscalar(lon), np.asscalar(lat)
        else:      return lon.reshape(x.shape), lat.reshape(y.shape)

    image2sphere = imageToSphere

def sphere2image(lon_ref,lat_ref,lon,lat):
    proj = Projector(lon_ref,lat_ref)
    return proj.sphere2image(lon,lat)

def image2sphere(lon_ref,lat_ref,x,y):
    proj = Projector(lon_ref,lat_ref)
    return proj.image2sphere(x,y)

def angsep(lon1,lat1,lon2,lat2):
    """
    Angular separation (deg) between two sky coordinates.
    Borrowed from astropy (www.astropy.org)
    Notes
    -----
    The angular separation is calculated using the Vincenty formula [1],
    which is slighly more complex and computationally expensive than
    some alternatives, but is stable at at all distances, including the
    poles and antipodes.
    [1] http://en.wikipedia.org/wiki/Great-circle_distance
    """
    lon1,lat1 = np.radians([lon1,lat1])
    lon2,lat2 = np.radians([lon2,lat2])
    
    sdlon = np.sin(lon2 - lon1)
    cdlon = np.cos(lon2 - lon1)
    slat1 = np.sin(lat1)
    slat2 = np.sin(lat2)
    clat1 = np.cos(lat1)
    clat2 = np.cos(lat2)

    num1 = clat2 * sdlon
    num2 = clat1 * slat2 - slat1 * clat2 * cdlon
    denominator = slat1 * slat2 + clat1 * clat2 * cdlon

    return np.degrees(np.arctan2(np.hypot(num1,num2), denominator))


def match(lon1, lat1, lon2, lat2, tol=None, nnearest=1):
    """
    Adapted from Eric Tollerud.
    Finds matches in one catalog to another.
 
    Parameters
    lon1 : array-like
        Longitude of the first catalog (degrees)
    lat1 : array-like
        Latitude of the first catalog (shape of array must match `lon1`)
    lon2 : array-like
        Longitude of the second catalog
    lat2 : array-like
        Latitude of the second catalog (shape of array must match `lon2`)
    tol : float or None, optional
        Proximity (degrees) of a match to count as a match.  If None,
        all nearest neighbors for the first catalog will be returned.
    nnearest : int, optional
        The nth neighbor to find.  E.g., 1 for the nearest nearby, 2 for the
        second nearest neighbor, etc.  Particularly useful if you want to get
        the nearest *non-self* neighbor of a catalog.  To do this, use:
        ``spherematch(lon, lat, lon, lat, nnearest=2)``
 
    Returns
    -------
    idx1 : int array
        Indices into the first catalog of the matches. Will never be
        larger than `lon1`/`lat1`.
    idx2 : int array
        Indices into the second catalog of the matches. Will never be
        larger than `lon2`/`lat2`.
    ds : float array
        Distance (in degrees) between the matches
    """
    from scipy.spatial import cKDTree
 
    lon1 = np.asarray(lon1)
    lat1 = np.asarray(lat1)
    lon2 = np.asarray(lon2)
    lat2 = np.asarray(lat2)
 
    if lon1.shape != lat1.shape:
        raise ValueError('lon1 and lat1 do not match!')
    if lon2.shape != lat2.shape:
        raise ValueError('lon2 and lat2 do not match!')

    rotator = SphericalRotator(0,0)

 
    # This is equivalent, but faster than just doing np.array([x1, y1, z1]).T
    x1, y1, z1 = rotator.cartesian(lon1.ravel(),lat1.ravel())
    coords1 = np.empty((x1.size, 3))
    coords1[:, 0] = x1
    coords1[:, 1] = y1
    coords1[:, 2] = z1
 
    x2, y2, z2 = rotator.cartesian(lon2.ravel(),lat2.ravel())
    coords2 = np.empty((x2.size, 3))
    coords2[:, 0] = x2
    coords2[:, 1] = y2
    coords2[:, 2] = z2
 
    tree = cKDTree(coords2)
    if nnearest == 1:
        idxs2 = tree.query(coords1)[1]
    elif nnearest > 1:
        idxs2 = tree.query(coords1, nnearest)[1][:, -1]
    else:
        raise ValueError('invalid nnearest ' + str(nnearest))
 
    ds = angsep(lon1, lat1, lon2[idxs2], lat2[idxs2])
 
    idxs1 = np.arange(lon1.size)
 
    if tol is not None:
        msk = ds < tol
        idxs1 = idxs1[msk]
        idxs2 = idxs2[msk]
        ds = ds[msk]
 
    return idxs1, idxs2, ds