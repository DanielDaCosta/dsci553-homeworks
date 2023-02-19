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
spark-submit task1.py ../resource/asnlib/publicdata/review.json task1.json
```
