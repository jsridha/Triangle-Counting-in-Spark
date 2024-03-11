Code author
-----------
Jay Sridharan

Installation
------------
The following components are installed:
- OpenJDK 11
- Hadoop 3.3.5
- Maven
- AWS CLI (for EMR execution)
- Scala 2.12.17
- Spark 3.4.0 (without bundled Hadoop)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop-3.3.5
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SCALA_HOME=/usr/share/scala
export SPARK_HOME=/usr/local/spark-3.3.2-bin-without-hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

Execution
---------
All of the build & execution commands are organized in the Makefile. Additionally, a sample dataset is included in the twitterdata.tar.gz archive.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
