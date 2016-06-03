#!/bin/bash

HATRAC_SERVER="https://gpcr-expt01.misd.isi.edu"

DATABASE=ermrest
GPCR_USER=ermrest
TMPFILE_NAME=getFileSources
SERVER_ROOT="/var/www"


	sudo su -c "psql -A -t ${DATABASE}" - ${GPCR_USER} <<EOF > ${TMPFILE_NAME}
	Select filename||','||uri from assets.fcs_source
EOF

	for readLine in $(cat ${TMPFILE_NAME})
	do
		FileName=$(echo ${readLine} | awk -F',' '{print $1}' )
		FileURI=$(echo ${readLine} | awk -F',' '{print $2}' )
		DirName=$(dirname ${FileURI})
		echo ${DirName}/$FileName
		echo ${SERVER_ROOT}$FileURI
		
		echo "curl -b ~/cookie -c ~/cookie -X PUT -H" "content-Type: application/octect-stream" -T "${SERVER_ROOT}${FileURI}" "${HATRAC_SERVER}${DirName}/${FileName}"
		curl -b ~/cookie -c ~/cookie -X PUT -H "content-Type: application/octect-stream" -T "${SERVER_ROOT}${FileURI}" "${HATRAC_SERVER}${DirName}/${FileName}" 
#exit
	done
		

