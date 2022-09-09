# sparkstarter
###### Installed version of Spark "https://spark.apache.org/downloads.html"
        -->  Spark release: 3.3.0   package type:Pre-built for Apache Hadoop 2.7

You need is to have java installed on your system PATH, or the JAVA_HOME environment variable pointing to a Java installation.
Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.7+ and R 3.5+.

[Optional]: Can also add the top-level Spark directory in env variables as "SPARK_HOME"

To run one of the Java or Scala sample programs, use bin/run-example <class> [params] in the top-level Spark directory.
$ .\bin\run-example SparkPi 10

To run Spark interactively through a modified version of the Scala shell.
$ ./bin/spark-shell --master local[2]

##### Changes Required:
        
Class : WordCount.java
-->     String logFile = "C:\\......\\sparkstarter\\src\\main\\resources\\spark_example.txt";
        Give absolute path for file ine src\main\resources\spark_example.txt
