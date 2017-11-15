#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from threading import Thread
from xml.dom.minidom import parseString

import urllib.parse as urlparse
import urllib.request as urlrequest
import requests, ssl, base64, json
import sys


class EASAccessor(object):
    '''
    Main class to encapsulate query jobs for EAS
    '''

    EAS_TAP_URL = "http://eas.esac.esa.int/tap-dev/tap/async"
    VOSpace_Url = 'https://vospace.esac.esa.int/vospace'

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
        self.request = urlrequest.Request(EASAccessor.EAS_TAP_URL, method="POST")
        self.request.add_header("Content-type", EASAccessor.Content_Type)
        self.request.add_header("Accept", EASAccessor.MIME_Text_Plain)

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
            print ("Status: " + phase)
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

    def set_auth(self, user, pwd):
        self.vospace_user = user
        self.vospace_pwd = pwd
        self.vospace_auth_set = True

    def save_to_vospace(self, folder, file, user, pwd):
        if user is None or pwd is None:
            if not self.vospace_auth_set:
                print ("ERROR: VOSpace credentials not provided")
                sys.exit(1)
            else:
                user = self.vospace_user
                pwd = self.vospace_pwd

        print ("Saving query results in VOSpace private account for user: " + user)

        transfer_url = EASAccessor.VOSpace_Url + '/servlet/transfers/async?PHASE=RUN'
        end_point = EASAccessor.VOSpace_Url + '/vospace/service/data/'

        metadataTransfer = 'transfer_push.xml'  # Contains the path where to store the output file
        toupload = file  # Name of the file to be uploaded in VOSpace

        txData = '''<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">
                        <vos:target>vos://esavo!vospace/platest</vos:target>
                        <vos:direction>pushToVoSpace</vos:direction>
                        <vos:view uri="vos://esavo!vospace/core#fits"/>
                        <vos:protocol uri="vos://esavo!vospace/core#httpput"/>
                    </vos:transfer>'''.format(folder)
        files = {'file': (metadataTransfer, txData)}

        try:
            upload_request = requests.post(transfer_url, files=files, auth=(user, pwd))
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print (upload_request.text)
            sys.exit(1)
        else:  # 200
            redirection = upload_request.url
            jobid = redirection[redirection.rfind('/') + 1:]

        upload_request.close()

        while True:
            request = requests.get(redirection, auth=(user, pwd))
            # XML response: parse it to obtain the current status
            data = request.text
            dom = parseString(data)
            phaseElement = dom.getElementsByTagName('uws:phase')[0]
            phaseValueElement = phaseElement.firstChild
            phase = phaseValueElement.toxml()
            print ("Status: " + phase)
            # Check finished
            if phase == 'COMPLETED': break
            if phase == 'ERROR': sys.exit(1)
            # wait and repeat
            sleep(0.3)

        # Open XML document using minidom parser
        DOMTree = parseString(data)
        collection = DOMTree.documentElement
        jobId_element = collection.getElementsByTagName('uws:jobId')[0]
        jobId = jobId_element.childNodes[0].data

        files = {'file': (toupload, self.results())}

        try:
            upload_post = requests.post(end_point + user + "/" + jobId, files=files, auth=(user, pwd))
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print (upload_post.text)
            sys.exit(1)
        else:  # 200
            # print(upload_post.text)
            redirection = upload_post.url
            jobid = redirection[redirection.rfind('/') + 1:]
            print ("Job id: " + jobid)
        upload_post.close()

        return upload_post.ok


def main():
    easHdl = EASAccessor()
    easHdl.setQuery(adqlQry="SELECT DISTANCE(POINT('ICRS', rightascension, declination), " +
                                   "POINT('ICRS',16.41683,4.90781)) AS dist, " +
                                    "* FROM sc3_mer_cat_10 " +
                            "WHERE 1=CONTAINS(POINT('ICRS', rightascension, declination)," +
                                             "CIRCLE('ICRS',16.41683,4.90781, 26)) " +
                            "ORDER BY dist ASC")
    isOK = easHdl.run()

    if isOK:
        print (easHdl.results())
        easHdl.save_to_vospace(folder='queries',
                               file='mi_query_results.csv',
                               user='myuser',
                               pwd='mypasswd')


if __name__ == '__main__':
    main()
