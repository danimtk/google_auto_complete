import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        String inputPath = args[0];
        String nGramLibPath = args[1];
        String numberOfNGram = args[2];
        String threashold = args[3];
        String topkFollowingWords = args[4];

        // job1
        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", "[.,;:!?'\"]");
        conf1.set("numGram", numberOfNGram);
        
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);
        
        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        TextInputFormat.setInputPaths(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLibPath));
        job1.waitForCompletion(true);
        
        // job2
        Configuration conf2 = new Configuration();
        conf2.set("threashold", threashold);
        conf2.set("topK", topkFollowingWords);
        
        // DBConfiguration.configureDB(conf2, 
        //         "com.mysql.jdbc.Driver",
        //         "jdbc:mysql://ip_address:port/database_name",
        //         "username",
        //         "password");
        DBConfiguration.configureDB(conf2, 
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.103:8889/autocomplete",
                "root",
                "root");
        
        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModel");
        job2.setJarByClass(Driver.class);

        // use "addArchiveToClassPath" method to 
        // define the dependency path on hdfs
        // job2.addArchiveToClassPath(new Path("hdfs_path_to_sql-connector"));
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        
        job2.setMapperClass(LanguageModel.LanguageModelMap.class);
        job2.setReducerClass(LanguageModel.LanguageModelReduce.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);
        
        DBOutputFormat.setOutput(job2, "output", 
                new String[] {"starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(job2, new Path(nGramLibPath));
        job2.waitForCompletion(true);
    }

}