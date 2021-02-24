import java.util.*;
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
import java.util.ArrayList;

public class JoinTable {
    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JoinTable");

        job.setJarByClass(JoinTable.class);

        job.setMapperClass(JoinTableMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinTableReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinTableMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable outputKey = new IntWritable();

            String[] record = value.toString().split(" ");
            int len = record.length;
            if(len == 3) {
                //记录来自product表
                String pid = record[0];
                outputKey.set(Integer.valueOf(pid));
            }
            if(len == 4) {
                //记录来自order表
                String pid = record[2];
                outputKey.set(Integer.valueOf(pid));
            }

            context.write(outputKey, value);
        }
    }

    public static class JoinTableReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text productInfo = new Text();
            List<Text> orders = new ArrayList<Text>();

            for(Text text: values) {ahoi
               String[] record = text.toString().split(" ");
               int len = record.length;
               if(len == 3) {
                   productInfo.set(new Text(text));
               }
               if(len == 4) {
                   orders.add(new Text(text));
               }
            }

            Text outputValue = new Text();
            for(Text text: orders) {
                String[] record = text.toString().split(" ");
                String id = record[0];
                String date = record[1];
                String pid = record[2];
                String num = record[3];

                //获取对应的商品信息
                String[] info = productInfo.toString().split(" ");
                String name = info[1];
                String price = info[2];

                //商品信息和订单信息组合成完整信息
                String fullInfo = id + " " + date + " " + pid + " " + name + " " + price + " " + num;
                outputValue.set(fullInfo);
                context.write(NullWritable.get(), outputValue);
            }
        }
    }
}
