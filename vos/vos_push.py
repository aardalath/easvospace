#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""EAS_Query class from package eas

This module incorporates almost without modification the code provided
by the ESDC Euclid team to store some content in a folder/file in the
user VOSpace account.

Usage:
    The sequence of commands to perform a query would be
     1. Create the VOSpace_Push object
     2. Call the ``save_to_file`` method to store something in a
        VOSpace folder/file

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
from xml.dom.minidom import parseString

import requests
import sys


class VOSpace_Push(object):
    '''
    Main class to encapsulate VOSpace storage functions
    '''

    VOSpace_Url = 'https://vospace.esac.esa.int/vospace'

    Tx_XML_File = """<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">
                        <vos:target>vos://esavo!vospace/{}/{}</vos:target>
                        <vos:direction>pushToVoSpace</vos:direction>
                        <vos:view uri="vos://esavo!vospace/core#fits"/>
                        <vos:protocol uri="vos://esavo!vospace/core#httpput"/>
                    </vos:transfer>"""

    def __init__(self):
        """Initialize object (class instance) attributes."""
        self.vospace_user = ""
        self.vospace_pwd = ""
        self.vospace_auth_set = False

    def set_auth(self, user, pwd):
        """Specifies the VOSpace user/passsword credentials to be used."""
        self.vospace_user = user
        self.vospace_pwd = pwd
        self.vospace_auth_set = True

    def save_to_file(self, folder, file, content, user=None, pwd=None):
        """Makes a storage request, followed by the sending the actual data to be
        stored in the desired folder/file.  The VOSpace user credentials are needed."""
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

        txData = VOSpace_Push.Tx_XML_File.format(user,folder)
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

        files = {'file': (toupload, content)}

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

    def save_file(self, folder, file, local_file, user, pwd):
        """Makes a storage request, followed by the sending the actual data to be
        stored in the desired folder/file.  The VOSpace user credentials are needed."""
        with open(local_file, "rb") as bin_file:
            # Read the whole file at once
            bin_data = bin_file.read()
        return self.save_to_file(folder, file, bin_data, user, pwd)


def main():
    """Sample usage of the VOSpace_Push class"""
    pass


if __name__ == '__main__':
    main()
