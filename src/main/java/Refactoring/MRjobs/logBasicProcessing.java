package Refactoring.MRjobs;

import java.io.File;  
import java.io.IOException;  
import java.util.HashSet;  
  
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;

import Refactoring.Mapper.LogMapper;
import Refactoring.Reducer.LogReducer;
import Refactoring.Mapper.classifyMulMapper;

public class logBasicProcessing {

	public static int main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        Configuration conf = new Configuration();  
        String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();  
        if (otherargs.length != 3) {  
            System.err.println("Usage logBasicProcessing <InputPath1> <InputPath2> <OutPath>");  
            System.exit(2);  
        }  
  
        // 创建基础作业  
        Job job1 = Job.getInstance(conf, logBasicProcessing.class.getSimpleName() + "LogAnalysis");  
        Job job2 = Job.getInstance(conf, logBasicProcessing.class.getSimpleName() + "classfyMR");  
  
        // Job1作业参数配置  
        job1.setJarByClass(logBasicProcessing.class);  
        job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(LogMapper.class);
		job1.setReducerClass(LogReducer.class);
		
        job1.setInputFormatClass(TextInputFormat.class);  
        job1.setOutputFormatClass(TextOutputFormat.class);  
        FileInputFormat.addInputPath(job1, new Path(otherargs[0]));  
        FileInputFormat.addInputPath(job1, new Path(otherargs[1])); 
        FileOutputFormat.setOutputPath(job1, new Path(otherargs[2]+File.separator+"mid"));  
  
        job2.setJarByClass(logBasicProcessing.class);  
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class); 
        job2.setMapperClass(classifyMulMapper.class);  
        //job2.setReducerClass(MyReducer3.class);  
        //job2.setInputFormatClass(KeyValueTextInputFormat.class);  
        //job2.setOutputFormatClass(TextOutputFormat.class);  
        FileInputFormat.addInputPath(job2, new Path(otherargs[2]+File.separator+"mid"));  
        FileOutputFormat.setOutputPath(job2, new Path(otherargs[2]+File.separator+"result"));  
  
        // 创建受控作业  
        ControlledJob cjob1 = new ControlledJob(conf);  
        ControlledJob cjob2 = new ControlledJob(conf);  
  
        // 将普通作业包装成受控作业  
        cjob1.setJob(job1);  
        cjob2.setJob(job2);  
  
        // 设置依赖关系  
        cjob2.addDependingJob(cjob1);  
  
        // 新建作业控制器  
        JobControl jc = new JobControl("My control job");  
  
        // 将受控作业添加到控制器中  
        jc.addJob(cjob1);  
        jc.addJob(cjob2);  
  
        /** 
         * hadoop的JobControl类实现了线程Runnable接口。我们需要实例化一个线程来让它启动。直接调用JobControl的run()方法，线程将无法结束。 
         */  
        //jc.run();  
          
        Thread jcThread = new Thread(jc);    
        jcThread.start();    
        while(true){    
            if(jc.allFinished()){    
                System.out.println(jc.getSuccessfulJobList());    
                jc.stop();    
                return 0;    
            }    
            if(jc.getFailedJobList().size() > 0){    
                System.out.println(jc.getFailedJobList());    
                jc.stop();    
                return 1;    
            }    
        }   
    }  

}
