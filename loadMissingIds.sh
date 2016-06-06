#!/bin/bash

DATABASE=ermrest
GPCR_USER=ermrest
PATH1=/bulk/FCS_DATA/FCS_processed_all
TMPFILE=tempBiomassLoad
if [ ${2} -eq 1 ]
then

	QrySt="insert into iobox_data.biomassprodbatch (id,site_prov) values("


	rm -f BiomassLoad.sql

	ls ${PATH1}/${1}*.FCS | cut -d "_" -f6 | sort | uniq >${TMPFILE}

	for readLine in $(cat ${TMPFILE})
	do
	#	echo ${readLine} | cut -d "_" -f3 | sort | uniq
		echo "${QrySt} "${readLine}" ,1);COMMIT;" >> BiomassLoad.sql	
	done
	sudo su -c "psql ${DATABASE} < /home/ankotha/bin/BiomassLoad.sql" - ${GPCR_USER}
	
elif [ ${2} -eq 2 ]
then
	QrySt="insert into iobox_data.construct (id,target,site_prov) values("


        rm -f TargetLoad.sql

        ls ${PATH1}/${1}*.FCS | cut -d "_" -f5 | sed 's/[^0-9]*//g' |sort | uniq >${TMPFILE}

        for readLine in $(cat ${TMPFILE})
        do
        #       echo ${readLine} | cut -d "_" -f3 | sort | uniq
                echo "${QrySt} "${readLine}" ,1,1);COMMIT;" >> TargetLoad.sql
        done

	sudo su -c "psql ${DATABASE} < /home/ankotha/bin/TargetLoad.sql" - ${GPCR_USER}
fi


