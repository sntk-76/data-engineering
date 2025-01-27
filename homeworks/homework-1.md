### Answer to Question 1
```bash
$ docker run -it --entrypoint bash python:3.12.8
root@081568d398f3:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
root@081568d398f3:/#
```

---

### Answer to Question 2
```
db:5432
```

---

### Answer to Question 3

#### Part 1: Trips Up to 1 Mile
```sql
SELECT COUNT(*) AS trips_up_to_1_mile
FROM green_taxi
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```
Result: **104830**

---

#### Part 2: Trips Between 1 and 3 Miles
```sql
SELECT COUNT(*) AS trips_between_1_and_3_miles
FROM green_table
WHERE trip_distance > 1 AND trip_distance <= 3
  AND lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```
Result: **198995**

---

#### Part 3: Trips Between 3 and 7 Miles
```sql
SELECT COUNT(*) AS trips_between_3_and_7_miles
FROM green_taxi
WHERE trip_distance > 3 AND trip_distance <= 7
  AND lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```
Result: **109642**

---

#### Part 4: Trips Between 7 and 10 Miles
```sql
SELECT COUNT(*) AS trips_between_7_and_10_miles
FROM green_taxi
WHERE trip_distance > 7 AND trip_distance <= 10
  AND lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```
Result: **2768**

---

#### Part 5: Trips Over 10 Miles
```sql
SELECT COUNT(*) AS trips_over_10_miles
FROM green_taxi
WHERE trip_distance > 10
  AND lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```
Result: **35201**

---

### Answer to Question 4

```sql
SELECT
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS longest_trip_distance
FROM
    green_table
WHERE
    DATE(lpep_pickup_datetime) IN ('2019-10-11', '2019-10-24', '2019-10-26', '2019-10-31')
GROUP BY
    DATE(lpep_pickup_datetime)
ORDER BY
    longest_trip_distance DESC
LIMIT 1;
```
Result: **2019-10-31**

---

### Answer to Question 5

```sql
SELECT
    z."Zone" AS pickup_location,
    SUM(g.total_amount) AS total_amount_sum
FROM
    green_taxi g
JOIN
    zone_data z
ON
    g."PULocationID" = z."LocationID"
WHERE
    DATE(g.lpep_pickup_datetime) = '2019-10-18'
GROUP BY
    z."Zone"
HAVING
    SUM(g.total_amount) > 13000
ORDER BY
    total_amount_sum DESC;
```
Result: **East Harlem North, East Harlem South, Morningside Heights**

---

### Answer to Question 6

```sql
SELECT
    z2."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS largest_tip
FROM
    green_taxi g
JOIN
    zone_data z1
ON
    g."PULocationID" = z1."LocationID"
JOIN
    zone_data z2
ON
    g."DOLocationID" = z2."LocationID"
WHERE
    z1."Zone" = 'East Harlem North'
    AND DATE(g.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY
    z2."Zone"
ORDER BY
    largest_tip DESC
LIMIT 1;
```
Result: **JFK Airport**

---

### Answer to Question 7
```
terraform init, terraform apply -auto-approve, terraform destroy
