# Competition

# Vocareum Set up
```bash
export PYSPARK_PYTHON=python3.6
```

```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

# Task

## Local

```bash
spark-submit competition.py ../resource/asnlib/publicdata ../resource/asnlib/publicdata/yelp_val.csv competition.csv
```


# Training

```bash
spark-submit train.py ../resource/asnlib/publicdata models_test
```
