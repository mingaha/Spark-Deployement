spark-class org.apache.spark.deploy.master.Master
spark-class org.apache.spark.deploy.worker.Worker spark://192.168.239.1:7077
spark-submit.cmd --class org.example.Main --master spark://192.168.239.1:7077 --conf spark.app.name="spark." --executor-cores 1 --executor-memory 1G D:\MS2D2\sara\Spark-Deployement\firstspark\target\sample-spark-1.0-SNAPSHOT.jar