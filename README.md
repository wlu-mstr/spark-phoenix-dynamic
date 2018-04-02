# Enable dynamic columns in spark-phoenix

## Phoenix dynamic feature
Given a table created with primary keys as well as some fixed common columns, we want to add dynamic columns in Spark.
Dynamic column in Spark is necessary for a generic data ingest system (based on Spark).

Table creation:
```sql
CREATE TABLE EventLog (
    eventId BIGINT NOT NULL,
    eventTime TIME NOT NULL,
    eventType CHAR(3)
    CONSTRAINT pk PRIMARY KEY (eventId, eventTime))
```

Upsert data:
```sql
UPSERT INTO EventLog (eventId, eventTime, eventType,
lastGCTime TIME, usedMemory BIGINT, maxMemory BIGINT)
VALUES(1, CURRENT_TIME(), 'abc', CURRENT_TIME(), 512, 1024);
```

## Enable RDD dynamic (example in `TestSp`)
RDD data:
```scala
  val dataSet = List((1017, "2018-01-01 12:00:00", "t1",1, 1024),
    (1027, "2018-01-01 12:00:00", "t2", 2, 2048),
    (1037, "2018-01-01 12:00:00", "t3", 3, 4096))
```
The last two fields are dynamic.
Field order in the RDD must be exactly same as order in the upsert sql:

```scala
val sql = "UPSERT  INTO EventLog (EVENTID, EVENTTIME, EVENTTYPE, maxMemory BIGINT, usedMemory BIGINT) VALUES (?, ?, ?, ?, ?)"
```
The upsert sql will be set into `Configuration` and later be used by `PhoenixRecordWriter` for phoenix upsert.

To make `saveToPhoenix` work for RDD, we also need to specify number of fields in the RDD, which is 5 in the example.

Query:
```
0: jdbc:phoenix:localhost> select * from EVENTLOG(maxMemory BIGINT, usedMemory BIGINT) where maxMemory is not null;
+----------+--------------------------+------------+------------+-------------+
| EVENTID  |        EVENTTIME         | EVENTTYPE  | MAXMEMORY  | USEDMEMORY  |
+----------+--------------------------+------------+------------+-------------+
| 1017     | 2018-01-01 12:00:00.000  | t1         | 1          | 1024        |
| 1027     | 2018-01-01 12:00:00.000  | t2         | 2          | 2048        |
| 1037     | 2018-01-01 12:00:00.000  | t3         | 3          | 4096        |
+----------+--------------------------+------------+------------+-------------+
3 rows selected (0.066 seconds)

```

## Enable DataFrame dynamic (example in `TestDf`)
When creating a DataFrame, we need to specify data type of each field at the first place. In the example, the raw input
is of Json type, where each field is with a primary data type.

```scala
val lines = List(
    """{"name":"Michael", "eventid":21, "eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}""",
    """{"eventid":22,"name":"Andy", "birth":"2018-01-01 12:00:00", "age":10,"eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}""",
    """{"eventid":23,"name":"Justin", "birth":"2018-01-01 13:00:00","eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}"""
  )
```
We can define data schema for each json schema:
```scala
  val configSchema = List(
    ("eventid", "integer"),
    ("eventtime", "timestamp"),
    ("eventtype", "string"),
    ("name", "string"),
    ("birth", "timestamp"),
    ("age", "integer")
  )
```
The field name order here is important, and they MUST be same as the upsert sql:
```scala
val sql = "UPSERT  INTO EventLog (EVENTID, EVENTTIME, EVENTTYPE, name CHAR(30), birth time, age BIGINT)
 VALUES (?, ?, ?, ?, ?, ?)"
```

Query:

```
0: jdbc:phoenix:localhost> select * from EVENTLOG(name CHAR(30), birth time, age BIGINT) where name is not null;
+----------+--------------------------+------------+----------+--------------------------+-------+
| EVENTID  |        EVENTTIME         | EVENTTYPE  |   NAME   |          BIRTH           |  AGE  |
+----------+--------------------------+------------+----------+--------------------------+-------+
| 21       | 2018-02-01 04:00:00.000  | tp         | Michael  |                          | null  |
| 22       | 2018-02-01 04:00:00.000  | tp         | Andy     | 2018-01-01 04:00:00.000  | 10    |
| 23       | 2018-02-01 04:00:00.000  | tp         | Justin   | 2018-01-01 05:00:00.000  | null  |
+----------+--------------------------+------------+----------+--------------------------+-------+
3 rows selected (0.082 seconds)

```
## Notes
These two Functions are hacked, and they are not robust.
There are several parts must be handled at user side:
- RDD data order in tuple must be same as that of upsert columns in sql;
- DataFrame data schema order nust be same as that of upsert columns in sql;
- Data type of dynamic columns in Rdd/DataFrame must be compatible to that of dynamic columns in upsert sql.