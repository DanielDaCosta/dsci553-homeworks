# Homework 2

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
spark-submit task1.py 2 4 ../resource/asnlib/publicdata/small1.csv text2.txt
```
## Vocareum

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task1.py 1 4 ../resource/asnlib/publicdata/small2.csv text.txt
```

# Task 2 

## Local

```bash
spark-submit task2.py 20 50 ../resource/asnlib/publicdata/ta_feng_all_months_merged.csv task2.txt
```

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py 20 50 ../resource/asnlib/publicdata/ta_feng_all_months_merged.csv task2.txt
```