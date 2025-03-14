### **Answer to Question 1**
```bash
rpk --version
rpk version v24.2.18 (rev f9a22d4430)
```
**Result:**
```
v24.2.18
```
---

### **Answer to Question 2**
```bash
NAME         PARTITIONS  REPLICAS
green-trips  1           1
```
---

### **Answer to Question 3**
```python
import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print(producer.bootstrap_connected())
```
**Result:**
```
True
```
---

### **Answer to Question 4**
```python
import time

t0 = time.time()

for index, row in data.iterrows(): 
    message = row.to_dict()
    producer.send(topic='green-trips', value=message)
    producer.flush()

t1 = time.time()
took = t1 - t0
print(f'The time taken for transmitting the data is: {took} seconds')
```
**Result:**
```
562.27 (approximately 9 minutes)
