#!/bin/bash
for q in q2 q3 q5 q8
do
   for i in {1..5}
   do
      echo "### Starting ${q} iteration $i"
      docker exec kafka /data/shared/kafka/bin/kafka-topics.sh --create --topic "p4${q}ss0$i" --bootstrap-server localhost:9092 --partitions 1
      sed -i -e "s/p4${q}ss0$((i-1))/p4${q}ss0$i/g" "/home/flink1/docker/data/shared/nexmark/queries/${q}.sql"
      if [ "$q" = "q2" ]
      then
         docker exec jm /data/shared/nexmark/bin/run_query.sh oa q2 "8:2"
      fi
      if [ "$q" = "q3" ]
      then
         docker exec jm /data/shared/nexmark/bin/run_query.sh oa q3 "8:3"
      fi
      if [ "$q" = "q5" ]
      then
         docker exec jm /data/shared/nexmark/bin/run_query.sh oa q5 "8:7"
      fi
      if [ "$q" = "q8" ]
      then
         docker exec jm /data/shared/nexmark/bin/run_query.sh oa q8 "8:16"
      fi
      /home/flink1/autoscale-experiment/autoscaler/rescale-scalein.sh
      sleep 60s
   done
done
