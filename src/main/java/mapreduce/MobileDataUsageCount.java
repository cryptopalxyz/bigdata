package mapreduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MobileDataUsageCount {


    static class TrafficCountMapper extends Mapper<LongWritable, Text, Text, MobileDataUsage> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 将一行内容转换成string
            String line = value.toString();
            // 切分字段
            String[] fields = line.split("\t");
            // 取出手机号
            String phoneNumber = fields[1];
            // 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);
            context.write(new Text(phoneNumber), new MobileDataUsage(upFlow, downFlow));
        }
    }


    static class TrafficeCountReducer extends Reducer<Text, MobileDataUsage, Text, MobileDataUsage> {
        @Override
        protected void reduce(Text key, Iterable<MobileDataUsage> values, Context context)
                throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
            // 遍历所有的bean,将其中的上行流量,下行流量分别累加
            for(MobileDataUsage bean : values) {
                sum_upFlow += bean.getUpstreamTraffic();
                sum_downFlow += bean.getDownstreamTraffic();
            }
            MobileDataUsage resultBean = new MobileDataUsage(sum_upFlow, sum_downFlow);
            context.write(key, resultBean);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MobileDataUsage.class);
        job.setMapperClass(TrafficCountMapper.class);
        job.setReducerClass(TrafficeCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MobileDataUsage.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MobileDataUsage.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

}
