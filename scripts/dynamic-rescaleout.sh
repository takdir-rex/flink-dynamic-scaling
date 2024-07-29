#!/bin/bash
docker exec jm /data/shared/nexmark/bin/run_query.sh oa "$1" "$2"
jobid=$(curl --location 'http://localhost:8081/jobs')
stat=""
x=0
until [ "$stat" = "RUNNING" ]
do
   x=$((x+1))
   stat=$(echo "$jobid" | cut -d $'"' -f$x)
   if [ "$stat" = "" ]
   then
       sleep 1s
       jobid=$(curl --location 'http://localhost:8081/jobs')
       x=0
   fi
done
x=$((x-4))
jobid=$(echo "$jobid" | cut -d $'"' -f$x)
echo "$jobid"
sleep 150s
curl --location --request PATCH "http://192.168.137.11:8081/jobs/$jobid/rescaling?vertex=$3&parallelism=$4"
sleep 100s
curl --location --request PATCH "http://192.168.137.11:8081/jobs/$jobid?mode=cancel"
