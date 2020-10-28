from astropy.coordinates import SkyCoord
try:
    from dustmaps.sfd import SFDQuery
except ModuleNotFoundError:
    print("The extinction_corr method is not available without first installing the dustmaps module:\n"
          "$> pip install --user dustmaps\n\n"
          "Then in a python interpreter:\n"
          ">>> import dustmaps.sfd\n"
          ">>> dustmaps.sfd.fetch()\n")


def extinction_corr(catalog, bands):

    # Extinction coefficients for HSC filters for conversion from E(B-V) to extinction, A_filter.
    # Numbers provided by Masayuki Tanaka (NAOJ).
    #
    # Band, A_filter/E(B-V)
    extinctionCoeffs_HSC = {
        "g": 3.240,
        "r": 2.276,
        "i": 1.633,
        "z": 1.263,
        "y": 1.075,
        "HSC-G": 3.240,
        "HSC-R": 2.276,
        "HSC-I": 1.633,
        "HSC-Z": 1.263,
        "HSC-Y": 1.075,
        "NB0387": 4.007,
        "NB0816": 1.458,
        "NB0921": 1.187,
    }

    sfd = SFDQuery()
    coord_string_ra = 'coord_ra_'+str(bands[0])
    coord_string_dec = 'coord_dec_'+str(bands[0])
    coords = SkyCoord(catalog[coord_string_ra], catalog[coord_string_dec])
    ebvValues = sfd(coords)
    extinction_dict = {'E(B-V)': ebvValues}

    # Create a dict with the extinction values for each band (and E(B-V), too):
    for band in bands:
        coeff_name = 'A_'+str(band)
        extinction_dict[coeff_name] = ebvValues*extinctionCoeffs_HSC[band]

    return(extinction_dict)
