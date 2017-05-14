
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGram {

    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

        int numOfGram;
        //setup方法只在初始化的时候被调用一次
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numOfGram = conf.getInt("numOfGram", 5);//这个project的numOfGram是从命令行读入
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
            input: read sentence
            I love data n=3
            I love -> 1
            love data -> 1
            I love data -> 1
            */

            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");//非字母的都会被替换成空格
            String[] words = line.split("\\s+");// "\\s"表示空格,回车,换行等空白符,"+"号表示一个或多个的意思

            // 不需要考虑1-gram的情况
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

    }
}


