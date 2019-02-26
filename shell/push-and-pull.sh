#!/bin/bash
#-----------------------------------------------------------------------------------
# push-and-pull.sh - Tests upload and download of simple file to/from VOSpace
#
# This script assumes the user/pwd shown below exist, as well as the test1 folder
# for that user
#-----------------------------------------------------------------------------------

#### Variables

line='----------------------------------------------------------------------'

user=eucops
pwd=Eu314_clid

filename=file.dat
file_to_upl=$(pwd)/${filename}
file_to_dwnl=$(pwd)/file-d.dat

url_vos=https://vospace.esac.esa.int/vospace
url_transfer=${url_vos}/servlet/transfers/async
url_upload=${url_vos}/servlet/data
url_download=${url_vos}/servlet/data/

CURL="curl -v -u ${user}:${pwd}"

# Create sample file
( echo $line; echo "This is the sample file"; date; echo $line ) | tee ${file_to_upl}

# Create job request files
cat << EOFpush > pushToVoSpace.xml
<?xml version="1.0" encoding="UTF-8"?>
<vos:transfer xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">
	<vos:target>vos://esavo!vospace/${user}/test1</vos:target>
	<vos:direction>pushToVoSpace</vos:direction>
	<vos:view uri="vos://esavo!vospace/core#fits"/>
	<vos:protocol uri="vos://esavo!vospace/core#httpput"/>
</vos:transfer>
EOFpush

cat << EOFpull > pullFromVoSpace.xml
<?xml version="1.0" encoding="UTF-8"?>
<vos:transfer  xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0">
	<vos:target>vos://esavo!vospace/${user}/test1/${filename}</vos:target>
	<vos:direction>pullFromVoSpace</vos:direction>
	<vos:view uri="vos://esavo!vospace/core#fits"/>
	<vos:protocol uri="vos://esavo!vospace/core#httpput"/>
</vos:transfer>
EOFpull

#### PUSH ######

echo "=============================================="
echo "  UPLOAD FILE"
echo "=============================================="

pushRegFile=$(pwd)/pushToVoSpace.xml

echo $line; echo "## 1 - JOB REGISTRATION"; echo $line

## 1 - Job registration
${CURL} -X POST -F upload=@${pushRegFile} "${url_transfer}?PHASE=RUN" 2>&1 | tr -d '\r' | tee pushreg.out

job_id=$(awk -F\/ '/Location:/{print $NF;}' pushreg.out)
echo "Job ID: ${job_id}"

sleep 2

echo $line; echo "## 2 - FILE UPLOAD"; echo $line

## 2 - Upload of file
${CURL} -X POST -F "file=@${file_to_upl}" "${url_upload}/${user}/${job_id}" 2>&1 | tr -d '\r' | tee pushupl.out

sleep 2

echo $line; echo "## 3 - JOB DELETION"; echo $line

## 3 - Job deletion
${CURL} -X DELETE "${url_transfer}/${job_id}" 2>&1 | tr -d '\r' | tee pushdel.out

sleep 5

#### PULL ######

echo "=============================================="
echo "  DOWNLOAD FILE"
echo "=============================================="

pullRegFile=$(pwd)/pullFromVoSpace.xml

echo $line; echo "## 1 - JOB REGISTRATION"; echo $line

## 1 - Job registration
${CURL} -X POST -F download=@${pullRegFile} "${url_transfer}?PHASE=RUN" 2>&1 | tr -d '\r' | tee pullreg.out

job_id=$(awk -F\/ '/Location:/{print $NF;}' pullreg.out)
echo "Job ID: ${job_id}"

sleep 2

echo $line; echo "## 2 - FILE UPLOAD"; echo $line

## 2 - Upload of file
${CURL} -X GET "${url_download}/${user}/${job_id}" > ${file_to_dwnl}

sleep 2

echo $line; echo "## 3 - JOB DELETION"; echo $line

## 3 - Job deletion
${CURL} -X DELETE "${url_transfer}/${job_id}" 2>&1 | tr -d '\r' | tee pulldel.out

echo $line ; echo "Comparing uploaded and downloaded files . . ."

cmp ${file_to_upl} ${file_to_dwnl} && echo "  => files are identical" || echo "  => files differ"

echo "Done."
