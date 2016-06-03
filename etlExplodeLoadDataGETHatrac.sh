#!/bin/bash

#Importing hatrac_util.sh file
. /home/ankotha/git/gpcr-project/etl-scripts/hatrac_util_mod.sh

DATABASE=ermrest
GPCR_USER=ermrest  #gpcr_policy
ASSETS_SCHEMA=assets
IOBOX_SCHEMA=iobox_data

LOG_DIR=/home/ankotha/logs
SUCCESS_LOG_NAME=Success.log
ERROR_LOG_NAME=Error.log


INPUT_DIR=/bulk/FCS_DATA/USC/FCS_processed
PROCESSED_FILES=/home/ankotha/processedFiles
TMP_DIR=/bulk/FCS_DATA/tmp
IN_HATRAC_FILE_DIR=/bulk/FCS_DATA/tmpHatrac
PROCESSED_DIR=/bulk/FCS_DATA/FCS_processed_all

HATRAC_SERVER=https://gpcr-expt01.misd.isi.edu
HATRAC_ROOT=/hatrac
TEMP_FILE_SOURCE=inputSourceFile.csv
ROOT_FOLDER=""
ERROR=1
: 'ERRORCODE=1 #Empty File
 	    =2 #Variable Not Assigned
	    =3 #Could not insert into the DB
	    =4 #No entry found in the DB
'


: '
getTargetID()            -->  generates the Target ID using the tables iobox_data.construct, iobox_data.site, iobox_data.target, iobox_data.targetlist
explodeFCS()	         -->  Uses "ProcessRawFCS utility to explode the FCS file into FCS,json,csv file"
getFileName()	         -->  Extracts the metadata from the file name and calls creataHatracnameSpace, InsertFileSourceDB, InsertFileStatDB()
createHatracNameSpace()  -->  Uses hatarac_util.sh to create the namespace on hatarac
InsertFileSourceDB()	 -->  Inserts data into assets.fcs_source
InsertFileStatDB	 -->  Inserts data into assets.fcs_stat table
getDigestfromDB()	 -->  gets the exisiting value of the digest fro the exisiting file and returns true or false
updateSourceStatusDB()	 -->  Insert data into assets.assets_status table with 1=new, updates the entry depending on the status 2=processed,3=retry and 4=error

Code Flow:
1. getSiteName
2. explodeFCS
3. getFileName #main controller
	i.	 getTargetId
	ii.  	 getSitePrefix
	iii.	 getDigestfromDB
	iv.	 upsertSourceStatusDB
	v.	 createHatracNameSpace
	vi.	 insertFileSourceDB
	vii.	 insertFileStatDB

'



writeLog(){

	logName=${1}
	errorCode=${2}
	fcs_mSHA=${3}
	fcs_iSHA=${4}
	rowId=${5}
	message=${6}
	echo `date` ' || ' ${errorCode}  ' || ' ${fcs_mSHA} ' || ' ${fcs_iSHA} ' || ' ${rowId} ' || ' ${message} >> ${logName} 
}



checkNullValue(){
	
	value=${1}
	name=${2}
 	
	if [ -n "${value}" ]
	then
		echo true
	else
		writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "2" "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "Unable to set the value for variable ${name} from file: ${files} ."
		echo false
	fi
	
}



checkFileSize(){
	srcFile="${1}"
	fileSize=$(stat -c %s "${srcFile}")
	
	if [ -n ${fileSize} -a ${fileSize} -gt 0 ]
	then
		echo true	
 	
	else
		echo false
		writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "1"  "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "No Source File To Load In File ${srcFile}. Exiting"
	
	fi
	

}




checkValuesDB(){
	tabName=${1}
	colValue=${2}
	qryString="Select count(1) from ${tabName} where id='${2}' and site_prov='${siteId}'";

	count=$(sudo su -c "psql -A -t ${DATABASE}" - ${GPCR_USER} <<EOF
		${qryString};
EOF
) 
	if [ -n ${count} -a ${count} -gt 0 ]
	then
		echo "true"
	else
		writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "4"  "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "No value exist for the table ${tabName} and id ${colValue} for file: ${files}"			
		echo "false"
	fi
	
}



getTargetId(){

         constructId=$1
         targetId=$(sudo su -c "psql -A -t ${DATABASE} " - ${GPCR_USER} <<EOF
                 Select /*(case when b.name = 'USC' then 'IMPT-'
                              when b.name = 'iHuman' then 'HUMM-'
                              when b.name = 'SIMM' then 'SIMM-'
                         end )*/'${prefix}'||'-'||d.name as targetid
                 from ${IOBOX_SCHEMA}.construct a join ${IOBOX_SCHEMA}.site b on (b.id=a.site_prov)
                         left outer join ${IOBOX_SCHEMA}.target c on (a.target=c.id and a.site_prov=c.site_prov)
                         left outer join ${IOBOX_SCHEMA}.targetlist d on (c.targetid=d.id)
                 where a.id='${constructId}'
                 and b.name='${siteName}'
EOF
)
	
	
        echo ${targetId}
}




explodeFCS()
{
	rawFileName="${1}"
	baseFileName="${2}"

	dir="${TMP_DIR}/${baseFileName}.FCS"
	mkdir -p ${dir}
	
	processRawFCS.py "${rawFileName}" "${dir}"
	
	if [ $? -eq 0 ]
	then
		baseName="${baseFileName}.FCS"
	
		cp "${dir}/${baseName}"* "${PROCESSED_DIR}"

		if [ $? -eq 0 ]
		then
			for files in "${PROCESSED_DIR}/${baseName}"*".FCS"
			do
				getFileName "${files}"
			done
	
		else

			echo "error Failed to copy the tmp files to processed all  dir"
			isErr="retry" 
		fi
	else
		echo "error in exploding files"
		isErr="retry"
		return ${ERROR}

	fi

	
}




getDigestfromDB(){
	newFileSHA=${1}
	

	existingCount=$(sudo su -c "psql -A -t ${DATABASE}"  - ${GPCR_USER} << EOF
			Select count(1) from ${ASSETS_SCHEMA}.fcs_file
			where sha256='${newFileSHA}'			
EOF
)
	

	if [ $? -eq 0 ]
	then
		if [ ${existingCount} -gt 0 ]
		then
			echo true
		else
			echo false
		fi
	else
		return ${ERROR}
	fi

}




updateSourceStatusDB() {

	fileNm=${1}
	statusType=${2}	
	

	sudo su -c "psql ${DATABASE}" - ${GPCR_USER} <<EOF
	update ${ASSETS_SCHEMA}.fcs_source
	set status_id=(Select id from ${ASSETS_SCHEMA}.asset_status where name='${statusType}'),
	last_modified_timestamp=now()::timestamp without time zone
	where filename='${fileNm}'
	and sha256='${FCSmultiSHA}'
EOF

}





getFileName(){
	echo "+++++++++++++++++Load File ${1} +++++++++++++++++" 	
	singleRawFileName="${1}"
	singleBaseFileName=$(basename "${singleRawFileName}" .FCS)
	echo "SingleRawFileName="${singleRawFileName}	
	OIFS=${IFS}
	IFS=' '
	set -- $( echo "${singleBaseFileName}" | awk -F "_" '{print $2,$3,$4}' )
	IFS=${OIFS}
	constructId=$(echo $1 | sed 's/[^0-9]*//g')
	biomassId=$(echo $2 | sed 's/[^0-9]*//g')
	indexId=$(echo $3 | sed 's/[^0-9]*//g')
	targetId=""

	if [ -n "${constructId}" ]
	then
		targetId=$(getTargetId ${constructId})

		 if [ ! -n "${targetId}"  ]
	        then
        	        isErr="retry"
                	echo "error"
	                 writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "4"  "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "Could Not set Value for TargetId for constructId=${constructId} for the file: ${files}"
        	        return ${ERROR}
	        fi

	fi
	
	
	echo "constructId=" ${constructId} "biomassId=" ${biomassId} "indexId=" ${indexId} "targetId="${targetId} "singleRawFileName=" ${singleRawFileName}
	
	FCSSingleSHA=""
	rowId=""
	rowId=${indexId}
	digestSHA256=$(sha256sum "${singleRawFileName}" | cut -d ' ' -f1)
	FCSSingleSHA=${digestSHA256}
	
	
	if [ $(checkNullValue "${constructId}" constructId) == "true" -a $(checkNullValue "${biomassId}" biomassId) == "true" -a $(checkNullValue "${indexId}" indexId) == "true" ]
	then
		
		if [ $(checkValuesDB ${IOBOX_SCHEMA}.construct ${constructId}) == "true" -a $(checkValuesDB ${IOBOX_SCHEMA}.biomassprodbatch ${biomassId}) == "true" ]		
		then

			digestSHA256=$(sha256sum "${singleRawFileName}" | cut -d ' ' -f1)
			checkDigestSHA256=$(getDigestfromDB ${digestSHA256})
			echo ${checkDigestSHA256}	
			if [ ${checkDigestSHA256} == false ]
			then
				URL=${HATRAC_ROOT}/target/${targetId}/construct/${prefix}"-"${constructId}/biomass/${prefix}"-"${biomassId}/${singleBaseFileName}
	                        fcsURL=${URL}.FCS
	                        jsonURL=${fcsURL}.json
			

				createHatracNameSpace "${singleBaseFileName}" ${targetId} ${constructId} ${biomassId} ${indexId} ${prefix}
				if [ $? -eq 0 ]
				then
					echo "Hatrac name Space create, SUCCESS"
						
					insertFileToDB ${constructId} ${biomassId} ${indexId} ${fcsURL} ${jsonURL} "${singleRawFileName}" ${digestSHA256}
	
				fi
			else
				echo "File already exists"
			fi
		else
			isErr="retry"
			echo "No entry in the DB found for construct biomass target. Check Error log for more detail"
			
		fi
	else
		echo "error"
		
		isErr="error" 
		echo "Null Values in Err for constructId biomassId indexId" ${constructId} ${biomassId} ${indexId}
		
	fi

}




createHatracNameSpace(){
	
	fileName="${1}"
	tId=${2}
	cId=${6}"-"${3}
	bId=${6}"-"${4}
	iId=${6}"-"${5}
	
	curl -s -f -b ~/cookie -c ~/cookie -X PUT -H "content-Type: application/x-hatrac-namespace" \
			"${HATRAC_SERVER}${HATRAC_ROOT}/target"\
			"${HATRAC_SERVER}${HATRAC_ROOT}/target/${tId}"\
			"${HATRAC_SERVER}${HATRAC_ROOT}/target/${tId}/construct"\
			"${HATRAC_SERVER}${HATRAC_ROOT}/target/${tId}/construct/${cId}"\
			"${HATRAC_SERVER}${HATRAC_ROOT}/target/${tId}/construct/${cId}/biomass"\
			"${HATRAC_SERVER}${HATRAC_ROOT}/target/${tId}/construct/${cId}/biomass/${bId}"
	
	hatracURL="${HATRAC_ROOT}/target/${tId}/construct/${cId}/biomass/${bId}/${fileName}"
	hatracFcsUrl="${hatracURL}.FCS"
	hatracFile="${PROCESSED_DIR}/${fileName}.FCS"
	fileSize=$(stat -c %s "${hatracFile}")
	if [ ${fileSize} -gt 0 ]
	then
		add_file_to_hatrac "${hatracFcsUrl}" "${hatracFile}"
		response=$?
		if [ ${response} -eq 0 ]
		then
			echo "${cId}, ${bId}, ${hatracFcsUrl}, SUCCESS"
	
		else
			echo "${cId}, ${bId}, ${hatracFcsUrl}, error"
			writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "Hatrac Error: ${response}"  "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "For URL: ${hatracURL} and File: ${hatracFile}"
			isErr="retry"
			return ${ERROR}
		fi
	else
		echo "error FCS empty"
		isErr="retry"
	fi

        hatracJsonUrl="${hatracURL}.json"
        fileSize=$(stat -c %s "${hatracFile}.json")
        if [ ${fileSize} -gt 0 ]
        then
                add_file_to_hatrac "${hatrac_url}" "${hatracFile}.json"
		response=$?
                if [ ${response} -eq 0 ]
                then
                        echo "${cId}, ${bId}, ${hatracJsonUrl}, SUCCESS"
                else
                        echo "${cId}, ${bId}, ${hatracJsonUrl}, error"
			writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "Hatrac Error: ${response}"  "${FCSmultiSHA}" "${FCSSingleSHA}" "${RowID}" "For URL: ${hatracURL} and File:  ${hatracFile}.json"
			isErr="retry"
			return ${ERROR}
                fi
	else
		echo "error json empty"
		isErr="retry"
		return ${ERROR}

        fi
		
}



insertFileToDB(){
#insertFileToDB ${constructId} ${biomassId} ${indexId} ${fcsURL} ${jsonURL} ${singleRawFileName} ${digestSHA256}

	constructId=${1}
        biomassId=${2}
        indexId=${3}
        fcsURL=${4}
        jsonURL=${5}
	sha256=${7}
	OrigFileName="${6}"
	fileName="${OrigFileName}.csv"
	
	sudo su -c "psql ${DATABASE}" - ${GPCR_USER} << EOF
	
        insert into ${ASSETS_SCHEMA}.fcs_file(site_id,construct_id,biomass_id,well_id,fcs_uri,json_uri,sha256)
        Select s.id,
        '${constructId}',
        '${biomassId}',
        '${indexId}',
        '${fcsURL}',
        '${jsonURL}',
        '${sha256}'
        from ${IOBOX_SCHEMA}.site s
        where s.name='${siteName}'
        and exists  (Select 1 from ${IOBOX_SCHEMA}.biomassprodbatch b
                     where b.id = ${biomassId}
                     and b.site_prov=${siteId})
        and not exists  (Select 1 from ${ASSETS_SCHEMA}.fcs_file f WHERE f.json_uri='${jsonURL}');


	
	Create temporary table stat_temp(
        quadrant serial PRIMARY KEY,
        percent_total text,
        mean text,
        median text,
        max text,
        min text,
        count text
        );

	COPY  stat_temp(percent_total,mean,median,max,min,count) from '${fileName}' WITH CSV HEADER;

	Insert into ${ASSETS_SCHEMA}.fcs_stats(quadrant,fcs_file,percent_total,mean,median,max,min,count)
	Select quadrant,
             (Select f.id from ${ASSETS_SCHEMA}.fcs_file f where f.json_uri='${jsonURL}') as fcs_file, 
	case when upper(trim(percent_total)) in ('NAN','') then null else trim(percent_total)::numeric end percent_total,
        case when upper(trim(mean)) in ('NAN','') then null else round(trim(mean)::numeric,2) end mean,
        case when upper(trim(median)) in ('NAN','') then null else round(trim(median)::numeric,2) end median,
        case when upper(trim(max)) in ('NAN','') then null else round(trim(max)::numeric,2) end max,
        case when upper(trim(min)) in ('NAN','') then null else round(trim(min)::numeric,2) end min,
        case when upper(trim(count)) in ('NAN','') then null else trim(count)::integer end count
        from stat_temp;

EOF

	if [ $? -eq 0 ]
	then 
		echo "Insert in ${ASSETS_SCHEMA}.fcs_stat, SUCCESS"
		writeLog "${LOG_DIR}/${SUCCESS_LOG_NAME}" "0" "${FCSmultiSHA}" "${sha256}" "${indexId}" "Data Loaded in Tables : ${ASSETS_SCHEMA}.fcs_file,${ASSETS_SCHEMA}.fcs_stats Data From: ${fileName}"
		                

	else
		echo "Insert in ${ASSETS_SCHEMA}.fcs_stat, error"
		 writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "3" "${FCSmultiSHA}" "${sha256}" "${indexId}" "Error Loading data in Tables : ${ASSETS_SCHEMA}.fcs_file,${ASSETS_SCHEMA}.fcs_stats Data From: ${fileName}"
		isErr="true"
		return ${ERROR}
	fi


}




getHatracFiles(){
	
	
	URI="${1}"
	outFile="${2}"

	curl -f -b cookie -c cookie -X GET "${URI}" --output "${IN_HATRAC_FILE_DIR}/${outFile}"
	cmdStatus=$?
	if [ ${cmdStatus} -eq 0 ]
	then
		 echo "true"
		
	else
		writeLog "${LOG_DIR}/${ERROR_LOG_NAME}" "Hatrac GET error : 1" "${FCSmultiSHA}" "${sha256}" "${indexId}" "Error in GET from hatrac system for ${URI}"
		echo "false" 
	fi
	
	
	return ${cmdStatus}

}



#Main Starts from here

sudo su -c "psql -A -t ${DATABASE}" - ${GPCR_USER} <<EOF > ${TEMP_FILE_SOURCE}
	Select '${HATRAC_SERVER}'||uri||','||site||','||sha256||','||
	       case when site = 'USC' then 'IMPT,1'
               	    when site = 'iHuman' then 'HUMM,2'
                    when site = 'SIMM' then 'SIMM,3'
		    else 'Error,-1'
               end||','||
	       filename
	from ${ASSETS_SCHEMA}.fcs_source
--	where filename='exptTest254.FCS'
	where status_id in (Select id 
			    from ${ASSETS_SCHEMA}.asset_status 
			    where name in ('retry','new')) limit 100;
EOF


	for readLine in $(cat inputSourceFile.csv)
	do
		#Reset Global Variables
		STM=$(date +%s)
		siteName=""
		FCSmultiSHA=""
		siteId=""
		prefix=""
		isErr="false"				

		echo "In For loop " ${readLine}
		OIFS=${IFS}
	        IFS=' '
	        set -- $( echo ${readLine} | awk -F "," '{print $1,$2,$3,$4,$5,$6}' )
	        IFS=${OIFS}
		
		inputFileName="${1}"
		siteName=${2}
		FCSmultiSHA=${3}
		prefix=${4}
		siteId=${5}
		outFile="${6}"

		
#		baseFileName=$(basename ${inputFileName} .FCS)
		FCSSingleSHA=""
		RowId=""
		ST=$(date +%s)		
		getFiles=$(getHatracFiles "${inputFileName}" "${outFile}" )
		ET=$(date +%s)
		echo "Seconds to download the file ${inputFileName}" i$((ET-ST))
		
		if [ ${getFiles} == "true" ]
		then
		
			inputFileName="${IN_HATRAC_FILE_DIR}/${outFile}"
			baseFileName=$(basename "${inputFileName}" .FCS)
			echo "AWK:" ${IN_HATRAC_FILE_DIR} ${inputFileName} ${siteName} ${FCSmultiSHA}
			chkSize=$(checkFileSize "${inputFileName}")
			echo "chksize="${chkSize}
			if [ ${chkSize} == true ]
			then
				explodeFCS "${inputFileName}" "${baseFileName}" 
	
				if [ ${isErr} !=  "false" ]
				then
					errSt=${isErr} 
				else
					errSt="processed"
				fi
	
				updateSourceStatusDB "${outFile}" ${errSt}
				
			fi
		else
			echo "error"
			errSt="retry"
			updateSourceStatusDB ${outFile} ${errSt}

		fi
		ETM=$(date +%s)
		echo "==================End Time for file ${readLine} is " $((ETM-STM)) "====================="
	done
