#ASYNCHRONOUS REQUEST
import urllib.parse
import urllib.request
import time
from xml.dom.minidom import parseString
import requests, ssl, base64, time, json

# Credentials used to access VOSpace account
user='ldap-user'
pssd='ldap-password'

url = "http://eas.esac.esa.int/tap-dev/tap/async"
contenttype = "application/x-www-form-urlencoded" 
textplain = "text/plain"

#-------------------------------------
#Create job

params = urllib.parse.urlencode({    "REQUEST": "doQuery",     "LANG":    "ADQL",     "FORMAT":  "csv",     "PHASE":  "RUN",     "JOBNAME":  "Any name (optional)",     "JOBDESCRIPTION":  "Any description (optional)",     "QUERY":   "SELECT DISTANCE(POINT('ICRS', rightascension, declination), POINT('ICRS',16.41683,4.90781)) AS dist, * FROM sc3_mer_cat_10 WHERE 1=CONTAINS(POINT('ICRS', rightascension, declination),CIRCLE('ICRS',16.41683,4.90781, 26)) ORDER BY dist ASC"
    })

request = urllib.request.Request(url, method="POST")
request.add_header("Content-type", contenttype)
request.add_header("Accept", textplain)
connection = urllib.request.urlopen(request, data=params.encode("UTF-8"))
#Status
print ("Status: " +str(connection.getcode()), "Reason: " + str(connection.reason))

#Server job location (URL)
info = connection.info()
redirection = connection.geturl()
#Jobid
jobid = redirection[redirection.rfind('/')+1:]
print ("Job id: " + jobid)

connection.close()

#-------------------------------------
#Check job status, wait until finished

while True:
    request = urllib.request.Request(redirection, method="GET")
    connection = urllib.request.urlopen(request)
    data = connection.read().decode("UTF-8")
    #XML response: parse it to obtain the current status
    dom = parseString(data)
    phaseElement = dom.getElementsByTagName('uws:phase')[0]
    phaseValueElement = phaseElement.firstChild
    phase = phaseValueElement.toxml()
    print ("Status: " + phase)
    #Check finished
    if phase == 'COMPLETED': break
    #wait and repeat
    time.sleep(0.3)

connection.close()

#-------------------------------------
#Get results
request = urllib.request.Request(redirection+"/results/result", method="GET")
connection = urllib.request.urlopen(request)



#-------------------------------------
#-------------------------------------
#VOSpace calls to save the file

print ("Saving query results in VOSpace private account for user: " + user)

transfer_url='https://vospace.esac.esa.int/vospace/servlet/transfers/async?PHASE=RUN'
end_point = "https://vospace.esac.esa.int/vospace/service/data/"
metadataTransfer = 'transfer_push_to_a.xml' # Contains the path where to store the output file
toupload = 'query_result_3.csv' # Name of the file to be uploaded in VOSpace

f = open(metadataTransfer, 'r', encoding="utf-8").read()
files = {'file': (metadataTransfer, f)}

try:
    upload_request = requests.post(transfer_url, files=files, auth=(user, pssd))
except requests.exceptions.RequestException as e:  # This is the correct syntax
    print (upload_request.text)
    sys.exit(1)
else: # 200
    redirection = upload_request.url
    jobid = redirection[redirection.rfind('/')+1:]

upload_request.close()
        
while True:
    request = requests.get(redirection, auth=(user, pssd))
    #XML response: parse it to obtain the current status
    data=request.text
    dom = parseString(data)
    phaseElement = dom.getElementsByTagName('uws:phase')[0]
    phaseValueElement = phaseElement.firstChild
    phase = phaseValueElement.toxml()
    print ("Status: " + phase)
    #Check finished
    if phase == 'COMPLETED': break
    if phase == 'ERROR' : sys.exit(1)
    #wait and repeat
    time.sleep(0.3)
    
# Open XML document using minidom parser
DOMTree = parseString(data)
collection = DOMTree.documentElement
jobId_element = collection.getElementsByTagName('uws:jobId')[0]
jobId = jobId_element.childNodes[0].data

f = connection.read()
files = {'file': (toupload, f)}

try:
    upload_post = requests.post(end_point + user + "/" + jobId, files=files, auth=(user, pssd))
except requests.exceptions.RequestException as e:  # This is the correct syntax
    print (upload_post.text)
    sys.exit(1)
else: # 200
    #print(upload_post.text)
    redirection = upload_post.url
    jobid = redirection[redirection.rfind('/')+1:]
    print ("Job id: " + jobid)
upload_post.close()
    
if upload_post.ok:
    print ("Done, Ok.........................................................");

connection.close()