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


def exec_query(adql, folder, file_name, user, pwd):
    easHdl = EAS_Query()
    easHdl.setQuery(adqlQry=adql)

    if not easHdl.run():
        print("ERROR while executing the query:\n{}".format(adql))
        sys.exit(1)

    easHdl.save_results_as_fits_table("results.fits")
    fits_data = easHdl.results_as_fits_table()

    #print(easHdl.results())
    vos = VOSpace_Handler()
    if not vos.save_to_file(folder=folder, file=file_name,
                            content=easHdl.results(),
                            user=user, pwd=pwd):
        print("ERROR while storing query results in VOSpace")
        sys.exit(2)

    vos.save_to_file(folder=folder, file=file_name + ".1.fits",
                     content=fits_data,
                     user=user, pwd=pwd)

    vos.save_file(folder=folder, file=file_name + ".2.fits",
                  local_file="results.fits",
                  user=user, pwd=pwd)

    vos.retrieve_file(folder='', file='queries',
                      local_file='queries.zip',
                      user=user, pwd=pwd)

    print("File '{}' with query results stored in your VOSpace folder {}".format(file_name, folder))


def main():

    exec_query(adql="SELECT DISTANCE(POINT('ICRS', rightascension, declination), " +
                                    "POINT('ICRS',16.41683,4.90781)) AS dist, " +
                           "* FROM sc3_mer_cat_10 " +
                    "WHERE 1=CONTAINS(POINT('ICRS', rightascension, declination), " +
                                     "CIRCLE('ICRS',16.41683,4.90781, 26)) " +
                    "ORDER BY dist ASC", 
               folder='queries', file_name='my_query_results.csv', 
               user='user', pwd='password')


if __name__ == '__main__':
    main()
