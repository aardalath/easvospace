from astropy.io import ascii,fits

def table2fits(tab, header=None):
    ''' 
        takes the data from an ascii.table.Table and outputs 
        a pyfits.hdu.table.BinTableHDU, creating a blank
        header if no header is provided
    '''
    if header is None:
        prihdr = fits.Header()
        prihdu = fits.PrimaryHDU(header=prihdr)
    else:
        prihdu = fits.PrimaryHDU(header=header)

    table_hdu = fits.BinTableHDU.from_columns(np.array(tab.filled()))

    return fits.HDUList([prihdu, table_hdu])

myasciitable = ascii.read("/Users/jcgonzalez/Downloads/my_query_results.csv")
myfitstable = table2fits(myasciitable)
myfitstable.writeto("/Users/jcgonzalez/Downloads/my_query_results.fits", clobber=True)
