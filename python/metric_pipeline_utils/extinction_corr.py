from lsst.sims.catUtils.dust.EBV import EBVbase as ebv


def extinction_corr(catalog, bands):

    # Extinction coefficients for HSC filters for conversion from E(B-V) to extinction, A_filter.                                                                            # Numbers provided by Masayuki Tanaka (NAOJ).                                                                                                                            #                                                                                                                                                                        # Band, A_filter/E(B-V)                                                                                                                                                   
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

    ebvObject = ebv()
    coord_string_ra = 'coord_ra_'+str(bands[0])
    coord_string_dec = 'coord_dec_'+str(bands[0])
    ebvValues = ebvObject.calculateEbv(equatorialCoordinates=np.array([catalog[coord_string_ra], catalog[coord_string_dec]]))
    extinction_dict = {'E(B-V)':ebvValues}

    # Create a dict with the extinction values for each band (and E(B-V), too):
    for band in bands:
        coeff_name = 'A_'+str(band)
        extinction_dict[coeff_name] = ebvValues*extinctionCoeffs_HSC[band]

    return(extinction_dict)
