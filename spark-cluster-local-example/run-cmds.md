#### Run commands

[Build the image]

```
docker compose build --no-cache
```

[Start the cluster with 3 workers]

```
docker compose up --scale spark-worker=3
```

[Submit the ETL job]

```
docker exec -it da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/apps/taxi_etl.py
```

[or]

```
docker exec -it da-spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/apps/taxi_etl.py
```

[Master UI]

```
http://localhost:9090
```

[History Server UI]

```
http://localhost:18080
```
