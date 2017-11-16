#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""EAS_Query class from package eas

This module incorporates almost without modification the code provided
by the ESDC Euclid team to preform am asynchronous query to the archives
via TAP+ interface.

Usage:
    The sequence of commands to perform a query would be
     1. Create the EAS_Query object
     2. Define the query with the ``setQuery()`` method
     3. Call the ``run()`` method, checking if the result is ``True``
     4. Retrieve the results with the ``results()`` method

    Please, have a look at the file ``query_and_save_to_vospace.py'' script for
    an example.  This example can be executed with::

        $ python query_and_save_to_vospace.py

"""

VERSION = '0.1.1'

__author__ = "jcgonzalez" # Refactoring from ESDC Euclid Team code
__credits__ = ["ESDC Euclid Team"]
__version__ = VERSION
__email__ = "jcgonzalez@sciops.esa.int"
__status__ = "Prototype" # Prototype | Development | Production


from time import sleep
from threading import Thread
from xml.dom.minidom import parseString

import urllib.parse as urlparse
import urllib.request as urlrequest


class EAS_Query(object):
    """
    Main class to encapsulate query jobs for EAS
    """

    EAS_TAP_URL = "http://eas.esac.esa.int/tap-dev/tap/async"

    Content_Type = "application/x-www-form-urlencoded"
    MIME_Text_Plain = "text/plain"

    def __init__(self):
        """Initialize object (class instance) attributes."""
        self.qry_params = None
        self.request = None
        self.connection = None
        self.status_info = ""
        self.jobThread = None

        self.vospace_user = ""
        self.vospace_pwd = ""
        self.vospace_auth_set = False

    def setQuery(self, adqlQry, name="myQuery", desc="This is my query"):
        """Define the query.  Multiple definitions are possible, but when run()
        is invoked, only the last one will be launched."""
        self.qry_params = urlparse.urlencode({"REQUEST":        "doQuery",
                                              "LANG":           "ADQL",
                                              "FORMAT":         "csv",
                                              "PHASE":          "RUN",
                                              "JOBNAME":        name,
                                              "JOBDESCRIPTION": desc,
                                              "QUERY":          adqlQry})
        self.request = urlrequest.Request(EAS_Query.EAS_TAP_URL, method="POST")
        self.request.add_header("Content-type", EAS_Query.Content_Type)
        self.request.add_header("Accept", EAS_Query.MIME_Text_Plain)

    def run(self):
        """Launch the last defined query.  The execution is done in a separate thread."""
        self.connection = urlrequest.urlopen(self.request, data=self.qry_params.encode("UTF-8"))
        self.qry_exit_code = self.connection.getcode()
        self.status_info = "Status: {}, Reason: {}".format(str(self.qry_exit_code),
                                                           str(self.connection.reason))
        self.connection_info = self.connection.info()
        self.connection_url = self.connection.geturl()
        self.jobid = self.connection_url[self.connection_url.rfind('/') + 1:]
        self.connection.close()
        self.jobThread = Thread(target=self.runUntilFinished, args=())
        self.jobThread.start()
        return self.qry_exit_code

    def runUntilFinished(self):
        """Performs the monitoring of the query requested, and retrieves the
        results for later use."""
        while True:
            self.request = urlrequest.Request(self.connection_url, method="GET")
            self.connection = urlrequest.urlopen(self.request)
            data = self.connection.read().decode("UTF-8")
            # XML response: parse it to obtain the current status
            dom = parseString(data)
            phaseElement = dom.getElementsByTagName('uws:phase')[0]
            phaseValueElement = phaseElement.firstChild
            phase = phaseValueElement.toxml()
            #print ("Status: " + phase)
            # Check finished
            if phase == 'COMPLETED': break
            # wait and repeat
            sleep(0.2)
        self.connection.close()

    def exit_info(self):
        """Return exit information in case the run() method reported a failure."""
        return self.status_info

    def results(self):
        """Returns the results from the last query executed."""
        # Wait for job to finish
        self.jobThread.join()
        # Retrieve and return results data
        self.request = urlrequest.Request(self.connection_url + "/results/result", method="GET")
        self.connection = urlrequest.urlopen(self.request)
        results_data = self.connection.read()
        self.connection.close()
        return results_data


def main():
    """Sample usage of the EAS_Query class"""
    pass # TODO


if __name__ == '__main__':
    main()
