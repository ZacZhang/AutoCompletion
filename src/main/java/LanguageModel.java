import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;


public class LanguageModel {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // input: I love big data\t10
        // output: key: I love big  value: data = 10

        int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context) throws IOException, InterruptedException {

            if ((value == null) || (value.toString().trim().length() == 0)) {
                return;
            }

            String line = value.toString().trim();

            String[] wordsPlusCount = line.split("\t");
            String[] words = wordsPlusCount[0].split("\\s+");
            int count = Integer.valueOf(wordsPlusCount[wordsPlusCount.length - 1]);

            if (wordsPlusCount.length < 2 || count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]);
                sb.append(" ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];
            if (!(outputKey.length() < 1)) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int topK;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            topK = configuration.getInt("topK", 5);
        }

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            // key: I love big
            // value: <data = 10, girl = 100, boy = 1000 ...>
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            // <10, <data, baby...>>, <100, <girl>>, <1000, <boy>>

            for (Text val : values) {
                // val: data = 10
                String value = val.toString().trim();
                String word = value.split("=")[0].trim();
                int count = Integer.parseInt(value.split("=")[1].trim());

                if (tm.containsKey(count)) {
                    tm.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();
            for (int j = 0; iter.hasNext() && j < topK; ) {
                int keyCount = iter.next();
                List<String> words = tm.get(keyCount);
                for (String curWord: words) {
                    context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
                    j++;
                }
            }
        }
    }
}