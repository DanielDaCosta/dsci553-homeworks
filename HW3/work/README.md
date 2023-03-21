# Homework 3

# Vocareum Set up
```bash
export PYSPARK_PYTHON=python3.6
```

```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

# Task 1

## Local

```bash
spark-submit task1.py ../resource/asnlib/publicdata/yelp_train.csv task1.csv
```
## Vocareum

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task1.py ../resource/asnlib/publicdata/yelp_train.csv task1.csv
```


# Task 2.1

## Local

```bash
spark-submit task2_1_v2.py ../resource/asnlib/publicdata/yelp_train.csv ../resource/asnlib/publicdata/yelp_val_in.csv task2_1.csv
```


# Task 2.2

## Local
```bash
spark-submit task2_2.py ../resource/asnlib/publicdata ../resource/asnlib/publicdata/yelp_val_in.csv task2_2.csv
```

# Task 2.3

## Local
```bash
spark-submit task2_2.py ../resource/asnlib/publicdata ../resource/asnlib/publicdata/yelp_val_in.csv task2_3.csv
```