# ly_poc

# Insert Data
```
spark-submit --class com.ibm.poc.PocInsertData --packages com.databricks:spark-csv_2.10:1.0.3 --master yarn-client --executor-memory 1G --num-executors 2 /data/dataupdater_2.10-0.1.0.jar yarn-client hdfs://edp01.esse.io:8020 /apps/hive/warehouse/ poc_merge /bryan/WT10000.csv
```

# Delete Data
```
spark-submit --class com.ibm.poc.PocDeleteData --packages com.databricks:spark-csv_2.10:1.0.3 --master yarn-client --executor-memory 1G --num-executors 2 /data/dataupdater_2.10-0.1.0.jar yarn-client hdfs://edp01.esse.io:8020 /apps/hive/warehouse/ poc_merge c1='WT10000'
```
```
spark-shell --packages com.databricks:spark-csv_2.10:1.0.3 --master yarn-client --executor-memory 1G --num-executors 2 --jars /data/dataupdater_2.10-0.1.0.jar
```

# Update data
```
spark-submit --class com.ibm.poc.PocUpdateData --packages com.databricks:spark-csv_2.10:1.0.3 --master yarn-client --executor-memory 1G --num-executors 2 /data/dataupdater_2.10-0.1.0.jar yarn-client hdfs://edp01.esse.io:8020 /apps/hive/warehouse/ poc_merge c1=\'WT00110\' C2=1
```

# Phoenix table mapping
```
create view "ph_test_snappy_153" (pk VARCHAR PRIMARY KEY,"f1".c1 varchar, "f2".c2 varchar, "f3".c3 varchar, "f4".c10 float, "f4".c11 float,"f4".c12 float,"f4".c13 float,"f4".c14 float,"f4".c15 float,"f4".c16 float,"f4".c17 float,"f4".c18 float,"f4".c19 float,"f4".c20 float,"f4".c21 float,"f4".c22 float,"f4".c23 float,"f4".c24 float,"f4".c25 float,"f4".c26 float,"f4".c27 float,"f4".c28 float,"f4".c29 float,"f4".c30 float,"f4".c31 float,"f4".c32 float,"f4".c33 float,"f4".c34 float,"f4".c35 float,"f4".c36 float,"f4".c37 float,"f4".c38 float,"f4".c39 float,"f4".c40 float,"f4".c41 float,"f4".c42 float,"f4".c43 float,"f4".c44 float,"f4".c45 float,"f4".c46 float,"f4".c47 float,"f4".c48 float,"f4".c49 float,"f4".c50 float,"f4".c51 float,"f4".c52 float,"f4".c53 float,"f4".c54 float,"f4".c55 float,"f4".c56 float,"f4".c57 float,"f4".c58 float,"f4".c59 float,"f4".c60 float,"f4".c61 float,"f4".c62 float,"f4".c63 float,"f4".c64 float,"f4".c65 float,"f4".c66 float,"f4".c67 float,"f4".c68 float,"f4".c69 float,"f4".c70 float,"f4".c71 float,"f4".c72 float,"f4".c73 float,"f4".c74 float,"f4".c75 float,"f4".c76 float,"f4".c77 float,"f4".c78 float,"f4".c79 float,"f4".c80 float,"f4".c81 float,"f4".c82 float,"f4".c83 float,"f4".c84 float,"f4".c85 float,"f4".c86 float,"f4".c87 float,"f4".c88 float,"f4".c89 float,"f4".c90 float,"f4".c91 float,"f4".c92 float,"f4".c93 float,"f4".c94 float,"f4".c95 float,"f4".c96 float,"f4".c97 float,"f4".c98 float,"f4".c99 float,"f4".c100 float,"f4".c101 float,"f4".c102 float,"f4".c103 float,"f4".c104 float,"f4".c105 float,"f4".c106 float,"f4".c107 float,"f4".c108 float,"f4".c109 float,"f4".c110 float,"f4".c111 float,"f4".c112 float,"f4".c113 float,"f4".c114 float,"f4".c115 float,"f4".c116 float,"f4".c117 float,"f4".c118 float,"f4".c119 float,"f4".c120 float,"f4".c121 float,"f4".c122 float,"f4".c123 float,"f4".c124 float,"f4".c125 float,"f4".c126 float,"f4".c127 float,"f4".c128 float,"f4".c129 float,"f4".c130 float,"f4".c131 float,"f4".c132 float,"f4".c133 float,"f4".c134 float,"f4".c135 float,"f4".c136 float,"f4".c137 float,"f4".c138 float,"f4".c139 float,"f4".c140 float,"f4".c141 float,"f4".c142 float,"f4".c143 float,"f4".c144 float,"f4".c145 float,"f4".c146 float,"f4".c147 float,"f4".c148 float,"f4".c149 float,"f4".c150 float,"f4".c151 float,"f4".c152 float,"f4".c153 float);

select count(*) from test-snappy-153
```