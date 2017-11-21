# EAStoVOSpace

This is a simplistic refactoring of the code provided by the ESDC team to
execute a query into the archives, retrieve the data and store them into
a VOSpace file.

Please, note that this is a Python 3 code.  Compatibility with Python 2
is still ongoing.

This is still a work in progress.  It can be considered as the version 0.1.3.

To test the two packages provided, just follow these steps:

1. Modify the code in `query_and_save_to_vospace.py` script, to change:
   * The folder and file name where the output will be stored
   * Your VOSpace user and password (LDAP)

2. Run the followig in the console:

       $ python3 query_and_save_to_vospace.py

A file with the name provided will be saved into the location your selected,
in your VOSpace account.


