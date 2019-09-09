# Spark-Python-Tutorial

Install

```
$ brew install apache-spark@2.3.2

==> Installing apache-spark@2.3.2 from eddies/spark-tap
==> Downloading https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
Already downloaded: /Users/has3ong/Library/Caches/Homebrew/downloads/df98174845f8d2418a685220c6bccc23ada4c07c3f11875ea1df5a44b296366a--spark-2.3.2-bin-hadoop2.7.tgz
🍺  /usr/local/Cellar/apache-spark@2.3.2/2.3.2: 1,019 files, 243.9MB, built in 6 seconds

$ pip install pyspark

$ java -version
openjdk version "1.8.0_222"
OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_222-b10)
OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.222-b10, mixed mode)

$ javac -version
javac 1.8.0_222
```


PySpark

```
$ pyspark
Python 3.7.4 (default, Jul  9 2019, 18:13:23)
[Clang 10.0.1 (clang-1001.0.46.4)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/usr/local/lib/python3.7/site-packages/pyspark/jars/spark-unsafe_2.11-2.4.4.jar) to method java.nio.Bits.unaligned()
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
19/09/09 17:33:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Python version 3.7.4 (default, Jul  9 2019 18:13:23)
SparkSession available as 'spark'.
```