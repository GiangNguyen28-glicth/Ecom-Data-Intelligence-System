spark-submit --master spark://spark-master:7077 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/opt/spark-events /opt/spark-apps/main.py
