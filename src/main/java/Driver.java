

import java.io.IOException;
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

public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        // inputDir
        // outputDir
        // NumOfGram
        // topK

        String inputDir = args[0];
        String outputDir = args[1];
        String numOfGram = args[2];
        String threshold = args[3];
        String topK = args[4];

        // first mapreduce
        Configuration configurationNGram = new Configuration();
        configurationNGram.set("textinputformat.recode.delimiter", ".");
        configurationNGram.set("numOfGram", numOfGram);

        Job jobNGram = Job.getInstance(configurationNGram);
        jobNGram.setJobName("NGram");
        jobNGram.setJarByClass(Driver.class);

        jobNGram.setMapperClass(NGram.NGramMapper.class);
        jobNGram.setReducerClass(NGram.NGramReducer.class);

        jobNGram.setOutputKeyClass(Text.class);
        jobNGram.setMapOutputValueClass(IntWritable.class);

        jobNGram.setInputFormatClass(TextInputFormat.class);
        jobNGram.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(jobNGram, new Path(inputDir));
        TextOutputFormat.setOutputPath(jobNGram, new Path(outputDir));
        jobNGram.waitForCompletion(true);

        // second mapreduce
        Configuration configurationLanguage = new Configuration();
        configurationLanguage.set("threshold", threshold);
        configurationLanguage.set("topK", topK);

        DBConfiguration.configureDB(configurationLanguage,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.135:8889/test",
                "root",
                "root");

        Job jobLanguage = Job.getInstance(configurationLanguage);
        jobLanguage.setJobName("LanguageModel");
        jobLanguage.setJarByClass(Driver.class);

        jobLanguage.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));


        jobLanguage.setMapperClass(LanguageModel.Map.class);
        jobLanguage.setReducerClass(LanguageModel.Reduce.class);

        jobLanguage.setMapOutputKeyClass(Text.class);
        jobLanguage.setMapOutputValueClass(Text.class);
        jobLanguage.setOutputKeyClass(DBOutputWritable.class);
        jobLanguage.setOutputValueClass(NullWritable.class);

        jobLanguage.setInputFormatClass(TextInputFormat.class);
        jobLanguage.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(
                jobLanguage,
                "output",
                new String[] { "starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(jobLanguage, new Path(args[1]));
        jobLanguage.waitForCompletion(true);
    }
}
