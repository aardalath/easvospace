#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from threading import Thread
from xml.dom.minidom import parseString

import urllib.parse as urlparse
import urllib.request as urlrequest
import requests, ssl, base64, json
import sys


class EAS_Query(object):
    '''
    Main class to encapsulate query jobs for EAS
    '''

    EAS_TAP_URL = "http://eas.esac.esa.int/tap-dev/tap/async"

    Content_Type = "application/x-www-form-urlencoded"
    MIME_Text_Plain = "text/plain"

    def __init__(self):
        self.qry_params = None
        self.request = None
        self.connection = None
        self.status_info = ""
        self.jobThread = None

        self.vospace_user = ""
        self.vospace_pwd = ""
        self.vospace_auth_set = False

    def setQuery(self, adqlQry, name="myQuery", desc="This is my query"):
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
        return self.status_info

    def results(self):
        # Wait for job to finish
        self.jobThread.join()
        # Retrieve and return results data
        self.request = urlrequest.Request(self.connection_url + "/results/result", method="GET")
        self.connection = urlrequest.urlopen(self.request)
        results_data = self.connection.read()
        self.connection.close()
        return results_data


def main():
    pass


if __name__ == '__main__':
    main()
