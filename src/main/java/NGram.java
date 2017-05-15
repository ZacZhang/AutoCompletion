
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGram {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int numOfGram;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numOfGram = conf.getInt("numOfGram", 5);
        }

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {
            /*
            input: read sentence
            I love data n=3
            I love -> 1
            love data -> 1
            I love data -> 1
            */

            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");

            if (words.length < 2) {
                return;
            }

            StringBuilder sb;
            for (int i = 0; i < words.length; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < numOfGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}


