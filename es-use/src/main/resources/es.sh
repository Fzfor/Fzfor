#!/bin/bash

es_home=/opt/module/elasticsearch-7.8.0
kibana_home=/opt/module/kibana-7.8.0
if [ $# -lt 1 ]
    then
    echo "USAGE:es.sh {start|stop}"
    exit
fi

case $1 in
"start")
    #启动 ES
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------启动 $i es-------"
        ssh $i "source /etc/profile;nohup ${es_home}/bin/elasticsearch >/dev/null 2>&1 &"
    done

    #启动kibana
    echo " --------启动 kibana-------"
    ssh hadoop102 nohup /opt/module/kibana-7.8.0/bin/kibana > /dev/null 2>&1 &
;;
"stop")
    #停止kibana
    echo " --------停止 kibana-------"
    ssh hadoop102 "sudo netstat -nltp | grep 5601 | awk '{print \$7}' | awk -F / '{print \$1}' | xargs -n1 kill"

    #停止 ES
    for i in hadoop102 hadoop103 hadoop104
    do
        echo " --------停止 $i es-------"
        ssh $i "ps -ef|grep $es_home |grep -v grep|awk '{print \$2}'|xargs kill"
        #ssh $i "jps | grep Elasticsearch | awk '{print \$1}' | xargs -n1 kill"
        #ssh $i "/opt/module/jdk1.8.0_212/bin/jps | grep Elasticsearch | awk '{print \$1}' | xargs -n1 kill"
    done
;;
*)
    echo "USAGE:es.sh {start|stop}"
    exit
;;
esac