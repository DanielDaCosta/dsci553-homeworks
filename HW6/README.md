# Homework 6

# Vocareum Set up
```bash
export PYSPARK_PYTHON=python3.6
```

```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

# Locally

Add python packages (e.g. sklearn, pandas...)
```bash
export PYSPARK_PYTHON=python
```

# Task 1

## Vocareum

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit task.py ../resource/asnlib/publicdata/hw6_clustering.txt 10 task.txt
```

## Locally

```bash
spark-submit task.py ../resource/asnlib/publicdata/hw6_clustering.txt 10 task.txt
```