# Homework 1

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
spark-submit task1.py ../resource/asnlib/publicdata/test_review.json task1.json
```

```bash
spark-submit task1.py ../resource/asnlib/publicdata/review.json task1.json
```

# Task 2 


## Vocareum
```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py ../resource/asnlib/publicdata/test_review.json task2.json 2
```

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py ../resource/asnlib/publicdata/review.json task2.json 2
```

# Task 3

## Local
```bash
spark-submit task3.py ../resource/asnlib/publicdata/test_review.json ../resource/asnlib/publicdata/business.json  output_question_a.txt output_question_b.json
```

## Vocareum
```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task3.py ../resource/asnlib/publicdata/test_review.json ../resource/asnlib/publicdata/business.json  output_question_a.txt output_question_b.json
```

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task3.py ../resource/asnlib/publicdata/review.json ../resource/asnlib/publicdata/business.json  output_question_a.txt output_question_b.json
```