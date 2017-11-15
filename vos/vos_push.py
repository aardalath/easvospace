#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from threading import Thread
from xml.dom.minidom import parseString

import urllib.parse as urlparse
import urllib.request as urlrequest
import requests, ssl, base64, json
import sys


class VOSpace_Push(object):
    '''
    Main class to encapsulate VOSpace storage functions
    '''

    VOSpace_Url = 'https://vospace.esac.esa.int/vospace'

    def __init__(self):
        self.vospace_user = ""
        self.vospace_pwd = ""
        self.vospace_auth_set = False

    def save_to_file(self, folder, file, data, user, pwd):
        if user is None or pwd is None:
            if not self.vospace_auth_set:
                print ("ERROR: VOSpace credentials not provided")
                sys.exit(1)
            else:
                user = self.vospace_user
                pwd = self.vospace_pwd

        #print ("Saving query results in VOSpace private account for user: " + user)

        transfer_url = VOSpace_Push.VOSpace_Url + '/servlet/transfers/async?PHASE=RUN'
        end_point = VOSpace_Push.VOSpace_Url + '/service/data/'

        metadataTransfer = 'transfer_pushi_to_a.xml'  # Contains the path where to store the output file
        toupload = file  # Name of the file to be uploaded in VOSpace

        txData = '''<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">
                        <vos:target>vos://esavo!vospace/{}/{}</vos:target>
                        <vos:direction>pushToVoSpace</vos:direction>
                        <vos:view uri="vos://esavo!vospace/core#fits"/>
                        <vos:protocol uri="vos://esavo!vospace/core#httpput"/>
                    </vos:transfer>'''.format(user,folder)
        #print(txData + '\n');
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
            #print (data + '\n')
            dom = parseString(data)
            phaseElement = dom.getElementsByTagName('uws:phase')[0]
            phaseValueElement = phaseElement.firstChild
            phase = phaseValueElement.toxml()
            #print ("Status: " + phase)
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

        files = {'file': (toupload, data)}

        try:
            upload_post = requests.post(end_point + user + "/" + jobId, files=files, auth=(user, pwd))
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print (upload_post.text)
            sys.exit(1)
        else:  # 200
            #print(upload_post.text)
            redirection = upload_post.url
            jobid = redirection[redirection.rfind('/') + 1:]
            #print ("Job id: " + jobid)
        result = upload_post.ok
        upload_post.close()
        return result


def main():
    pass


if __name__ == '__main__':
    main()
