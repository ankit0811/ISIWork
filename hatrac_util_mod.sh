#!/bin/sh

# error code
SUCCESS=0
NOT_EXIST=1
UNAUTHORIZED=2
DUPLICATED=3
ERROR=99

hex2base64()
{
    # decode stdin hex digits to binary and recode to base64
    xxd -r -p | base64
}

hatrac_md5sum()
{
    # take data on stdin and output base64 encoded hash
    md5sum | sed -e "s/ \+-//" | hex2base64
}

RESPONSE_HEADERS=/tmp/response-headers
RESPONSE_CONTENT=/tmp/response-content
COOKIES=cookie

cleanup()
{
    rm -f ${RESPONSE_HEADERS} ${RESPONSE_CONTENT} 
}

trap cleanup 0

mycurl()
{
    touch ${RESPONSE_HEADERS}
    touch ${RESPONSE_CONTENT}
    truncate -s 0 ${RESPONSE_HEADERS}
    truncate -s 0 ${RESPONSE_CONTENT}
    
    curl_options=(
      -D ${RESPONSE_HEADERS}
      -o ${RESPONSE_CONTENT}
      -w "%{http_code}::%{content_type}::%{size_download}\n"
      -s -k
      -b "$COOKIES" -c "$COOKIES"
    )


    curl "${curl_options[@]}" "$@"
}

get_existing_md5()
{
	#echo "1111=="$1
	#chk_hatrac_access ${1}
	chkAccess=$(chk_hatrac_access ${1})
	#echo "chkAccess="  ${chkAccess}
	#exit
	if [ -n "${chkAccess}" -a ${chkAccess} != 99 -a ${chkAccess} != 3 ]
	then
	
		curl_options=(
		-s -k -f
		-b "$COOKIES" -c "$COOKIES"
		-I  # HEAD
		)

	

		md5=$(curl "${curl_options[@]}" "$1" \
			| grep -i '^Content-MD5:' \
			| sed -e 's/[ \r]*$//' \
			| sed -e 's/^[^:]\+: *//')
		echo ${md5}

# TODO ankit: improve this    
#    head=$( curl "${curl_options[@]}" "$1" )
#    if [[ $? -eq 0 ]]
#    then
	# check
#	
#    else
#	return ${ERROR}
#    fi
	else
		return ${chkAccess}
	fi
}



chk_hatrac_access(){
	curl_options=(
        	-s -k
	        -b "$COOKIES" -c "$COOKIES"
        	-I  # HEAD
    	)

	responseValue=$(curl "${curl_options[@]}" "$1" | grep -i '^HTTP' | cut -d " " -f2)
	#echo "responseValue="${responseValue}
	
	#echo "In return part"
	if [ -n "${responseValue}" ]
	then
		if [ ${responseValue} == 403 ]
		then
			echo ${UNAUTHORIZED}
			return ${UNAUTHORIZED}	

		elif [ ${responseValue} == 401 -o ${responseValue} == 200 -o ${responseValue}==404 ]
		then
			echo ${SUCCESS} 
			return ${SUCCESS}
	
		else
			echo ${ERROR}
			return ${ERROR}
		fi
	else
		echo ${ERROR}
                return ${ERROR}

	fi
	#echo "Out return part"
	
}





# check if whether the check sum of the existing url is the same as
# the provided filename. If not, add the file to hatrac. 
# Arguments:
#   - hatrac_url
#   - filename
add_file_to_hatrac()
{

    hatrac_url="$1"
    filename="$2"
    mimetype=$(file --mime-type -b "${filename}")
    
    my_md5=$( hatrac_md5sum < "${filename}")
    existing_md5=$( get_existing_md5 "${HATRAC_SERVER}${hatrac_url}" )
	
  echo ${existing_md5} " and "	${my_md5}
#	get_existing_md5 "${HATRAC_SERVER}${hatrac_url}" 
#	exit

    echo "** url: ${HATRAC_SERVER} : ${hatrac_url} :${my_md5}:${existing_md5}:"

    result="1"
    if [ -n "${my_md5}" ]
    then
	
	if [[ "${my_md5}" != "${existing_md5}" ]]
	then
		curl -s -f -b cookie -c cookie -X PUT \
	     	-H "content-Type: ${mimetype}" \
	     	-H "content-MD5: ${my_md5}" \
	     	-T "${filename}" \
	     	"${HATRAC_SERVER}${hatrac_url}" 
		result=$?
		if [[ ${result} -eq 0 ]]
		then
	    		echo "+ ${result} : Added ${filename} to ${HATRAC_SERVER}${hatrac_url}"
		else
	    		echo "- ${result} : ERROR:CURL ${filename} ${HATRAC_SERVER}${hatrac_url}"
	    	result="99"
		fi
    	else
		echo "- ${result} : ERROR: file already exist"
    	fi
    else
	result="99"
	echo "- ${result} : ERROR: Unable to set the values for varialbe my_md5 and existing_md5"
    fi
    return ${result}
}


test()
{
    checksum=$( get_existing_md5 "https://dev.gpcrconsortium.org/hatrac/test/target/HUMM-CNR1/construct/HUMM-10/HUMM-10_alignment.html" )

    if [[ $? -eq 0 ]]
    then
	echo "Checksum available"
	md5=$( hatrac_md5sum < "HUMM-10_alignment.html" )
	if [[ "${checksum}" = "${md5}" && -n "${checksum}" ]]
	then
	    echo "same checksum"
	else
	    echo "mismatch"
	fi
    else
	echo "No checksum"
    fi
}


#test
