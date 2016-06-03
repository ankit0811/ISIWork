#!/bin/bash


insert_st="Insert into assets.fcs_source(site,filename,uri,sha256,status_id) values("
uriPath="/hatrac/assets/USC/fcs/2016/"

i=1

for readLine in $(cat inFileList)
do
	echo $readLine | awk -F ":" '{print $1}'
	filename="expt_Test_"${i}".FCS"
	
	
	
	uri=${uriPath}${readLine}
	sha256=$(echo ${readLine} | cut -d "." -f1)
	
	
	echo ${insert_st} "'USC','${filename}','${uri}','${sha256}','1');"
		
	sudo su -c "psql ermrest" - ermrest <<EOF
	${insert_st}'USC','${filename}','${uri}','${sha256}','1'); 
EOF

i=$((${i}+1))
		
done 
