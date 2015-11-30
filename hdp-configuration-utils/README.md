hdp-configuration-utils
=================

hdp-configuration-utils

Use the hdp-configuration-utils script to calculate the various parameters for memory in YARN,
MapReduce 2 and Hive.

./hdp-configuration-utils --help for more info.

Example:

hdp-configuration-utils.py -c 16 -m 64 -d 4 -k True


by default the script uses the following:

cores = 16 (-c option)
memory = 64 (-m option)
disks = 4 (-d option)
hbaseEnabled (-k option)
