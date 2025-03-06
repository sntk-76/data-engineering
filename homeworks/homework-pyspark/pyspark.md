### **Answer to Question 1**
```python

import pandas
from pyspark.sql import SparkSession


spark = SparkSession.builder \
        .master("local[*]") \
        .appName("homework") \
        .getOrCreate()


!pyspark --version

# Output:
# version 3.5.5
```

---

### **Answer to Question 2**
```python

!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet -O ./data/yellow_tripdata_2024-10.parquet


df = spark.read.parquet("./data/yellow_tripdata_2024-10.parquet")


df_repartitioned = df.repartition(4)


df_repartitioned.write.parquet("./data/partitions", mode="overwrite")


!ls -lh ./data/partitions
```

**Output**:
```
total 90M
-rw-r--r-- 1 sinatavakoli2022 sinatavakoli2022   0 Mar  6 10:15 _SUCCESS
-rw-r--r-- 1 sinatavakoli2022 sinatavakoli2022 23M Mar  6 10:15 part-00000-642969be-0a29-49b9-838d-a663aa68c550-c000.snappy.parquet
-rw-r--r-- 1 sinatavakoli2022 sinatavakoli2022 23M Mar  6 10:15 part-00001-642969be-0a29-49b9-838d-a663aa68c550-c000.snappy.parquet
-rw-r--r-- 1 sinatavakoli2022 sinatavakoli2022 23M Mar  6 10:15 part-00002-642969be-0a29-49b9-838d-a663aa68c550-c000.snappy.parquet
-rw-r--r-- 1 sinatavakoli2022 sinatavakoli2022 23M Mar  6 10:15 part-00003-642969be-0a29-49b9-838d-a663aa68c550-c000.snappy.parquet
```

---

### **Answer to Question 3**
```python

df_spark.createOrReplaceTempView("yellow_taxi")


spark.sql('''
SELECT COUNT(*) AS trip_count
FROM yellow_taxi 
WHERE DATE(tpep_pickup_datetime) = '2024-10-15'
''').show()
```

**Output**:
```
+-----------+
|trip_count |
+-----------+
|    128893 |
+-----------+
```

---

### **Answer to Question 4**
```python

spark.sql('''
SELECT 
    MAX((UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600) AS longest_trip_hours
FROM yellow_taxi
''').show()
```

**Output**:
```
+------------------+
| longest_trip_hours |
+------------------+
| 23.78           |
+------------------+
```

---

### **Answer to Question 5**
```
Answer: 4040
```

---

### **Answer to Question 6**
```python

spark.sql('''
SELECT 
    z.Zone, 
    COUNT(y.PULocationID) AS trip_count
FROM yellow_taxi y
JOIN zones z ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trip_count ASC
LIMIT 1
''').show()
```

**Output**:
```
+-----------------------------------------------+------------+
| Zone                                          | trip_count |
+-----------------------------------------------+------------+
| Governor's Island/Ellis Island/Liberty Island | 1          |
+-----------------------------------------------+------------+
```

---