#!/bin/sh
#set -x
function get_suffix() {
    num=$1
    suffix="000"
    if [[ $num -ge 100 ]]; then
        suffix="00"
    fi
    if [[ $num -ge 1000 ]]; then
        suffix="0"
    fi
    echo $suffix
}

function restart_services() {
    echo "Restarting services...."
    ssh edp06 service hadoop-yarn-resourcemanager restart;
    ssh edp06 service hadoop-hdfs-namenode restart;
    for i in {1..6} ; do ssh edp0$i "service hadoop-hdfs-datanode restart; service hadoop-yarn-nodemanager restart"; done
    for i in {1..6} ; do ssh edp0$i "free -m && sync && echo 3 > /proc/sys/vm/drop_caches && free -m"; done
}

echo Start: `date`
source="/poc_test/WT"
target="/poc_merge_tmp/"
target1="/poc_merge_tmp1/"
hdfs="hdfs://edp06:8020"
spark_cmd="/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit \
    --packages com.databricks:spark-csv_2.10:1.0.3 \
    --executor-memory 10G \
    --num-executors 12 \
    --total-executor-cores 20 \
    --executor-cores 4 \
    --driver-library-path /usr/lib/hadoop/lib/native/ \
    --master yarn-client \
    --properties-file ./spark.conf"
for i in {10..3009..30}; do
    suffix=`get_suffix $i`
    files=$hdfs/$source$suffix$i.csv
    for j in {1..29}; do
        num=$(($i + $j))
        if [[ $num -le 3009 ]]; then
            suffix=`get_suffix $num`
            file=$hdfs/$source$suffix$num.csv
            files=$files,$file
        fi
        if [[ num -eq 2000 ]]; then
            restart_services
            sleep 10
        fi
    done
    echo "Import: $files"
    $spark_cmd \
    --class io.esse.poc.POC_Load_CSV \
    /data/poc-project_2.10-0.1.0.jar \
    yarn-client hdfs://edp06:8020 "$files" $target > /root/poc/log$i.txt 2>&1
    rs=$?
    grep -n "OutOfMemory" /root/poc/log$i.txt
    rs1=$?
    # if run submit failed or find out of memory, need restart server
    if [[ rs -eq 1 ]] || [[ rs1 -eq 0 ]]; then
        restart_services
        mv /root/poc/log$i.txt /root/poc/log$i.failed
        sleep 10
    fi
    sleep 1
done
$spark_cmd \
--class io.esse.poc.Merge_Parquet \
/data/poc-project_2.10-0.1.0.jar \
yarn-client hdfs://edp06:8020 $target/*.parquet $target1 300 > /root/poc/log_merge_parquet.txt 2>&1
echo Finished: `date`
