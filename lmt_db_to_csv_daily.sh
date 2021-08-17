#!/bin/sh

year="2021"
month=$(printf "%02d" 8)
day=$(printf "%02d" 9)

target_date=${year}"-"${month}"-"${day}
path="/scratch/snuteam/perftest/log/LMT/"

# MDS_DATA
mysql -uroot -h10.141.100.1 -e "select B.TIMESTAMP, A.PCT_CPU, A.KBYTES_FREE, A.KBYTES_USED, A.INODES_FREE, A.INODES_USED from (MDS_DATA A JOIN TIMESTAMP_INFO B ON A.TS_ID = B.TS_ID) WHERE B.TIMESTAMP >= '${target_date} 00:00:00' AND B.TIMESTAMP <= '${target_date} 23:59:59';" -p0000 filesystem_lustre0 | tr '\t' ',' > ${path}${target_date}_MDS_D.csv

# MDS_OPS_DATA
mysql -uroot -h10.141.100.1 -e "select B.TIMESTAMP, A.OPERATION_ID, A.SAMPLES, A.SUM, A.SUMSQUARES from (MDS_OPS_DATA A JOIN TIMESTAMP_INFO B ON A.TS_ID = B.TS_ID) WHERE B.TIMESTAMP >= '${target_date} 00:00:00' AND B.TIMESTAMP <= '${target_date} 23:59:59';" -p0000 filesystem_lustre0 | tr '\t' ',' > ${path}${target_date}_MDS_OPS_D.csv

#OSS_DATA
mysql -uroot -h10.141.100.1 -e "select B.TIMESTAMP, A.PCT_CPU, A.PCT_MEMORY from (OSS_DATA A JOIN TIMESTAMP_INFO B ON A.TS_ID = B.TS_ID) WHERE B.TIMESTAMP >= '${target_date} 00:00:00' AND B.TIMESTAMP <= '${target_date} 23:59:59';" -p0000 filesystem_lustre0 | tr '\t' ',' > ${path}${target_date}_OSS_D.csv

#OST_DATA
mysql -uroot -h10.141.100.1 -e "select B.TIMESTAMP, A.READ_BYTES, A.WRITE_BYTES, A.KBYTES_FREE, A.KBYTES_USED, A.INODES_FREE, A.INODES_USED from (OST_DATA A JOIN TIMESTAMP_INFO B ON A.TS_ID = B.TS_ID) WHERE B.TIMESTAMP >= '${target_date} 00:00:00' AND B.TIMESTAMP <= '${target_date} 23:59:59';" -p0000 filesystem_lustre0 | tr '\t' ',' > ${path}${target_date}_OST_D.csv

#OST_ID_DATA
mysql -uroot -h10.141.100.1 -e "select B.TIMESTAMP, A.OST_ID, A.READ_BYTES, A.WRITE_BYTES, A.KBYTES_FREE, A.KBYTES_USED, A.INODES_FREE, A.INODES_USED from (OST_DATA A JOIN TIMESTAMP_INFO B ON A.TS_ID = B.TS_ID) WHERE B.TIMESTAMP >= '${target_date} 00:00:00' AND B.TIMESTAMP <= '${target_date} 23:59:59';" -p0000 filesystem_lustre0 | tr '\t' ',' > ${path}${target_date}_OST_ID_D.csv
