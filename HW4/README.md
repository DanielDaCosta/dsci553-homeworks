# Homework 4

# Vocareum Set up
```bash
export PYSPARK_PYTHON=python3.6
```

```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

# Task 1

## Vocareum

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 --executor-memory 4G --driver-memory 4G task1.py 7 ../resource/asnlib/publicdata/ub_sample_data.csv task1.txt
```


# Task 2

## Vocareum

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py 7 ../resource/asnlib/publicdata/ub_sample_data.csv task2_1.txt task2_2.txt
```

## Local
```bash
spark-submit task2.py 7 ../resource/asnlib/publicdata/ub_sample_data.csv task2_1.txt task2_2.txt
```