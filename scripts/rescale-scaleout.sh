#!/bin/bash
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
res=$(curl --location "http://192.168.137.11:8081/jobs/$jobid/savepoints" --header "Content-Type: application/json" --data "{}")
trigid=$(echo "$res" | cut -d $'"' -f 4)
stat=""
until [ "$stat" = "COMPLETED" ]
do
   res=$(curl --location "http://192.168.137.11:8081/jobs/$jobid/savepoints/$trigid")
   stat=$(echo "$res" | cut -d $'"' -f 6)
   sleep 1s
done
docker exec jm ssh root@tm202 "source /etc/profile; /data/shared/flink/bin/taskmanager.sh start"
docker exec jm ssh root@tm302 "source /etc/profile; /data/shared/flink/bin/taskmanager.sh start"
sleep 150s
curl --location --request PATCH "http://192.168.137.11:8081/jobs/$jobid?mode=cancel"
sleep 15s
docker exec jm ssh root@tm202 "source /etc/profile; /data/shared/flink/bin/taskmanager.sh stop"
docker exec jm ssh root@tm302 "source /etc/profile; /data/shared/flink/bin/taskmanager.sh stop"
