#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sample usage of EAS_Query and VOSpace_Push classes

Usage:
    Please, have a look at the packages ``eas`` and ``vos``.  This example can
    be executed with::

        $ python query_and_save_to_vospace.py

"""


from eas.eas_qry import EAS_Query
from vos.vos_handler import VOSpace_Handler

import sys


def main():
    """
    Sample code:
    - run queries
    - store csv and fits files in VOSpace
    - retrieve an entire folder from VOSpace
    """

    # Define query and request parameters
    adql = '''SELECT rightascension, declination, fluxgextdecamaper, fluxvisaper, fluxrextdecamaper, 
                     fluxiextdecamaper, fluxzextdecamaper,  fluxyaper, fluxjaper, fluxhaper
              FROM public.sc3_mer 
              WHERE (fluxgextdecamaper > 10)
              AND  starflag=0
              ORDER by fluxyaper DESC'''

    user = 'eucops'
    pwd = 'Eu314_clid'

    # Perform query
    easHdl = EAS_Query()
    easHdl.setQuery(adqlQry=adql)

    if not easHdl.run():
        print("ERROR while executing the query:\n{}".format(adql))
        sys.exit(1)

    print("- Query executed")

    # Store query retrieved data (in CSV format) as a FITS table
    local_fits = 'results.fits'
    easHdl.save_results_as_fits_table(local_fits)
    fits_data = easHdl.results_as_fits_table()

    print("- File '{}' with query results stored as FITS in current folder".format(local_fits))

    # Upload query results to VOSpace as a CSV file
    vos = VOSpace_Handler()
    folder = 'queries'
    file_name = 'my_query_results.csv'
    if not vos.save_to_file(folder=folder, file=file_name,
                            content=easHdl.results(),
                            user=user, pwd=pwd):
        print("ERROR while storing query results in VOSpace")
        sys.exit(2)

    print("- File '{}' with query results stored in your VOSpace folder {}".format(file_name, folder))

    # Upload the results in FITS format to a FITS file
    vos.save_to_file(folder=folder, file=file_name + ".1.fits",
                     content=fits_data,
                     user=user, pwd=pwd)

    print("- File '{}' with query results as FITS stored in your VOSpace folder {}".format(file_name + ".1.fits", folder))

    # Upload the already saved FITS file with another name
    # (the content should be the same as in the previous step)
    vos.save_file(folder=folder, file=file_name + ".2.fits",
                  local_file=local_fits,
                  user=user, pwd=pwd)

    print("- Local file '{}' uploaded as '{}' to your VOSpace folder {}".format(local_fits, file_name + ".2.fits", folder))

    # Retrive the entire folder 'queries' with all the uploaded data, as a ZIP file
    zip_file = 'queries.zip'
    vos.retrieve_file(folder='queries', file='',
                      local_file=zip_file,
                      user=user, pwd=pwd)

    print("- Folder '{}' from VOSpace retrieved as file '{}'".format(folder, zip_file))


if __name__ == '__main__':
    main()
