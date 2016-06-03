#!/bin/bash

TMP_DIR=/bulk/FCS_DATA/tmp
TMPFILE=LoadStatTmp.log
STATFILE=LoadStats

sudo su -c "psql -A -t ermrest" - ermrest <<EOF > ${TMPFILE}
with temp as
(Select '%'||filename||'%' likename ,status_id
from assets.fcs_source
where status_id not in (1)
)
Select status_id,temp.likename,count(fcs_uri) load_cnt
from assets.fcs_file,temp
where fcs_uri like likename
group by 1,2
order by 1,2;
EOF


echo "status_id|filename|successLoad|TotalLoad|err" > ${STATFILE}
totalErr=0
totalSucc=0
totalFiles=0
totalCount=0
for readLine in $(cat ${TMPFILE})
do
	
	#echo ${readLine}
	dirName=$(echo ${readLine} | sed 's/%//g' | awk -F "|" '{print $2}')
	#echo ${dirName}
	successCount=$(echo ${readLine} | awk -F "|" '{print $3}')
	TotalFile=$(ls -l ${TMP_DIR}/${dirName}/*.FCS | wc -l)

	totalErr=$((${totalErr}+$((${TotalFile}-${successCount}))))
	totalSucc=$((${totalSucc}+${successCount}))
	totalFiles=$((${totalFiles}+1))	
	totalCount=$((${TotalFile}+${totalCount}))
	echo ${readLine}"|"${TotalFile}"|"$((${TotalFile}-${successCount})) >> ${STATFILE}
done

rowsWith5Null=$(sudo su -c "psql -A -t ermrest" - ermrest <<EOF
		Select count(1) from (
		Select fcs_file,count(1) cnt
		from assets.fcs_stats
		where percent_total is null
		group by 1
		having count(1)=5
		)foo
		
EOF
)



#totalFiles=$(echo ${totlaFiles}*1.0 | bc -l )
	
echo "===================Summary===================">>${STATFILE}
echo "Total Files="${totalFiles} >> ${STATFILE}
echo "Total Success Rows="${totalSucc} >> ${STATFILE}
echo "Total Fail Rows="${totalErr} >>${STATFILE}
echo "Total Files with null stats data="${rowsWith5Null} >> ${STATFILE}
out=`echo ${totalCount}/${totalFiles} | bc -l`
echo "Average Rows Per File="${out} >>${STATFILE}  #$((${totalCount}/${totalFiles})) >>${STATFILE}
out=`echo ${totalSucc}/${totalFiles} | bc -l`
echo "Average Success Rows Per File="${out} >>${STATFILE} #$((${totalSucc}/${totalFiles})) >>${STATFILE}
out=`echo ${totalErr}/${totalFiles} | bc -l`
echo "Average Fail Rows="${out} >> ${STATFILE}  #$((${totalErr}/${totalFiles})) >>${STATFILE}



tail -8 LoadStats |  mailx -a "LoadStats" -s "Test Load Statistics `date`" ankit@isi.edu

