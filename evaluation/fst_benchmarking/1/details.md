# Notes
The following were the settings and results used to evaluate FST index compression.

# Runs
32400 rows
churn = 0.001
block size = 10000 data points
3 blocks
fst_total: 1333264
original_total: 1351444

32400 rows
churn = 0.01
block size = 10000 data points
3 blocks
fst_total: 1440846
original_total: 1465982

32400 rows
churn = 0.1
block size = 10000 data points
3 blocks
fst_total: 1808499
original_total: 2097632

32400 rows
churn = 0.001
block size = 30000 data points
1 block
fst_total: 1144632
original_total: 1161163

32400 rows
churn = 0.01
block size = 30000 data points
1 block
fst_total: 1218109
original_total: 1265668

32400 rows
churn = 0.1
block size = 30000 data points
1 block
fst_total: 1591373
original_total: 1878703

300000 rows
churn = 0.1
block size = 100000 data points
3 blocks
total_fst: 12982435
total_original: 15895504
savings: 0.18220700532761397

300000 rows
churn = 0.01
block size = 100000 data points
3 blocks
total_fst: 9515111
total_original: 10006367
savings: 0.04909434163268247

300000 rows
churn = 0.001
block size = 100000 data points
3 blocks
total_fst: 8706687
total_original: 8880449
savings: 0.019566803435276753
