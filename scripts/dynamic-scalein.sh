#!/bin/bash
for q in q2 q3 q4 q8
do
   for i in {1..5}
   do
      echo "### Starting ${q} iteration $i"
      docker exec kafka /data/shared/kafka/bin/kafka-topics.sh --create --topic "p4${q}ds0$i" --bootstrap-server localhost:9092 --p>
      sed -i -e "s/p4${q}ds0$((i-1))/p4${q}ds0$i/g" "/home/flink1/docker/data/shared/nexmark/queries/${q}.sql"
      if [ "$q" = "q2" ]
      then
        /home/flink1/autoscale-experiment/autoscaler/dynamic-rescalein.sh q2 "8:2"  0a448493b4782967b150582570326227 4
      fi
      if [ "$q" = "q3" ]
      then
        /home/flink1/autoscale-experiment/autoscaler/dynamic-rescalein.sh q3 "8:3" ea632d67b7d595e5b851708ae9ad79d6 4
      fi
      if [ "$q" = "q5" ]
      then
        /home/flink1/autoscale-experiment/autoscaler/dynamic-rescalein.sh q5 "8:7" 46f8730428df9ecd6d7318a02bdc405e 4
      fi
      if [ "$q" = "q8" ]
      then
        /home/flink1/autoscale-experiment/autoscaler/dynamic-rescalein.sh q8 "8:16" abfdc4d62ca3442d035b9ce3103c8291 4
      fi
      sleep 60s
   done
done
