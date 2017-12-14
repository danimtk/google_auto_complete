hdfs dfs -rm -r /mysql
hdfs dfs -mkdir /mysql

file="../dependency/mysql-connector-java-5.1.39-bin.jar"
if [ -f "$file" ]
then
    echo "$file found."
    hdfs dfs -put ../dependency/mysql-connector-java-*.jar /mysql/
else
    wget -c https://s3-us-west-2.amazonaws.com/wengaoye/mysql-connector-java-5.1.39-bin.jar
    hdfs dfs -put ./mysql-connector-java-*.jar /mysql/
fi

hdfs dfs -rm -r /input
hdfs dfs -mkdir /input
hdfs dfs -rm -r /output
hdfs dfs -put ../dataset/* /input/

hadoop com.sun.tools.javac.Main *.java
jar cf ngram.jar *.class
hadoop jar ngram.jar Driver /input /output 6 3 5
