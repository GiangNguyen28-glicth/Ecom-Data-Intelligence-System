/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=file:/opt/spark-events \
/opt/spark-apps/main.py

/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
/opt/spark-apps/main.py

/opt/spark/bin/spark-submit \
--master local[*] \
--driver-memory 2g \
/opt/spark-apps/main.py


dbt docs generate
dbt docs serve --host 0.0.0.0 --port 8080

snapshots iceberg

snapshot_id: ID của snapshot
parent_id: snapshot cha (giúp trace lineage)
operation: loại operation (append, overwrite, delete, …)
committed_at: thời gian commit
manifest_list: đường dẫn manifest
summary: metadata (record count, file count,...)

remove all container
docker rm -f $(docker ps -aq)