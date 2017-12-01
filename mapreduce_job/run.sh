file="../dependency/mysql-connector-java-5.1.39-bin.jar"
if [ -f "$file" ]
then
    echo "$file found."
else
    wget -c https://s3-us-west-2.amazonaws.com/wengaoye/mysql-connector-java-5.1.39-bin.jar
fi

hdfs dfs -rm -r /mysql
hdfs dfs -mkdir /mysql
hdfs dfs -put ../dependency/mysql-connector-java-*.jar /mysql/

hdfs dfs -rm -r /input
hdfs dfs -mkdir /input
hdfs dfs -rm -r /output
hdfs dfs -mkdir /output
hdfs dfs -put ../dataset/* /input/

hadoop com.sun.tools.javac.Main *.java
jar cf ngram.jar *.class
hadoop jar ngram.jar Driver /input /output 6 3 5
