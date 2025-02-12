### Answer to Question 1

```sql
SELECT COUNT(*) AS total_records
FROM `HW_3.yellow_taxi`;
```
Result: **20332093**

---

### Answer to Question 2

```sql
SELECT COUNT(DISTINCT(PULocationID)) AS distinct_PULocationID
FROM `HW_3.external_table`;

SELECT COUNT(DISTINCT(PULocationID)) AS distinct_PULocationID
FROM `HW_3.regular_table`;
```
Result: **0 MB for the External Table and 155.12 MB for the Materialized Table**

---

### Answer to Question 3

```sql
SELECT PULocationID
FROM `HW_3.regular_table`;

SELECT PULocationID, DOLocationID  
FROM `HW_3.regular_table`;

SELECT PULocationID
FROM `HW_3.regular_table`;
```
Result: **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**

---

### Answer to Question 4

```sql
SELECT COUNT(fare_amount)
FROM `HW_3.regular_table`
WHERE fare_amount = 0;
```
Result: **8333**

---

### Answer to Question 5

```sql
CREATE TABLE `ny-taxi-project-448909.HW_3.optimized_table`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `HW_3.regular_table`;
```
Result: **Partition by tpep_dropoff_datetime and Cluster on VendorID**

---

### Answer to Question 6

```sql
SELECT DISTINCT(VendorID)
FROM `HW_3.regular_table`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `HW_3.optimized_table`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
Result: **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

---

### Answer to Question 7

Result: **GCP Bucket**

---

### Answer to Question 8

Result: **False**

---