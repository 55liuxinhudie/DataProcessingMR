package Refactoring.MRjobs;

import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

import Refactoring.Mapper.filterMapper;
import Refactoring.Reducer.filterReducer;  
  
public class LogFilter {  
  
    public static class MyMapper extends  
            Mapper<Object, Text, Text, IntWritable> {  
        public void map(Object key, Text value, Context context)  
                throws IOException, InterruptedException {  
            context.write(value, new IntWritable());// 这里不能为NULL，只能是new  
                                                    // IntWritable()，不然会报空指针异常  
        }  
    }  
  
    public static class MyReducer extends  
            Reducer<Text, IntWritable, Text, IntWritable> {  
        public void reduce(Text key, Iterable<IntWritable> values,  
                Context context) throws IOException, InterruptedException {  
            context.write(key, null);// 这里则可以是为null，写入文件的value值为空，也就就是什么都不写，只写键  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
  
        String[] otherArgs = new GenericOptionsParser(conf, args)  
                .getRemainingArgs();  
        if (otherArgs.length != 2) {  
            System.err.println("Usage: wordcount <in> <out>");  
            System.exit(2);  
        }  

		Job job = Job.getInstance(conf,"LogFilter");
		job.setJarByClass(LogAnalysis.class);
		job.setJobName("LogFilter");
		
        job.setMapperClass(filterMapper.class);  
        job.setReducerClass(filterReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
  
}
