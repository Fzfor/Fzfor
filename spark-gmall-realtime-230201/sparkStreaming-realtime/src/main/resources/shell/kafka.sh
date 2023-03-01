#!/bin/bash
if [ $# -lt 1 ]
then
 echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
 exit
fi
case $1 in

"start"){
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------启动 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties "
    done
};;
"stop"){
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------停止 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh stop"
    done
};;
kc)
if [ $2 ]
then
 kafka-console-consumer.sh --bootstrap-server hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic $2
else
 echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
fi
;;
kp)
if [ $2 ]
then
 kafka-console-producer.sh --broker-list hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic $2
else
 echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
fi
;;
list)
kafka-topics.sh --list --bootstrap-server hadoop102:9092,hadoop103:9092,hadoop104:9092
;;
describe)
if [ $2 ]
then
kafka-topics.sh --describe --bootstrap-server hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic $2
else
echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
fi
;;
delete)
if [ $2 ]
then
kafka-topics.sh --delete --bootstrap-server hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic $2
else
echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
fi
;;
*)
 echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
 exit
;;
esac
