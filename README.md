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