package Refactoring.MRjobs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.tools.internal.jxc.gen.config.Config;

import Refactoring.Common.tools;

 public class MODEL {
   
   //第一个Job的map函数
   public static class Map_First extends Mapper<Object, Text, Text, IntWritable>{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
        public void map(Object key,Text value, Context context ) throws IOException, InterruptedException {
        	context.write(value, new IntWritable());
        }
    }
  
   //第一个Job的reduce函数
    public static class Reduce_First extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key,Iterable<IntWritable>values, Context context) throws IOException, InterruptedException {
    	  context.write(key, null);
      }
    }
    
    //第二个job的map函数
    public static class Map_Second extends Mapper<LongWritable, Text, Text, Text>{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
        private final static IntWritable one = new IntWritable(1);
        private Text keys = new Text();
        public void map(Object key,Text value, Context context ) throws IOException, InterruptedException {
        	String line = value.toString();
    		
    		String [] dataList = line.split("\\|_\\|");		
    		String ipString = dataList[0];
    		String timeString = dataList[2];
    		String urlString = dataList[3];
    		ArrayList stringList = new ArrayList<String>();
    		stringList.add(ipString);
    		stringList.add(timeString);
    		String outputString = tools.join(stringList, "\0005");
    		context.write(new Text(key.toString()), new Text(outputString));
    		
        }
    }
    
    //第二个Job的reduce函数
    public static class Reduce_Second extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable result = new IntWritable();
      public void reduce(Text key,Iterable<IntWritable>values, Context context) throws IOException, InterruptedException {
         int sum = 0;
         for(IntWritable value:values) {
           sum  +=  value.get();
         }
          result.set(sum);
          context.write(key, result);
      }
    }
    
    //启动函数
    public static void main(String[] args) throws IOException {
    
    JobConf conf = new JobConf(MODEL.class);
    
    //第一个job的配置
    @SuppressWarnings("deprecation")
	Job job1 = new Job(conf,"join1");
    job1.setJarByClass(MODEL.class); 

      job1.setMapperClass(Map_First.class); 
      job1.setReducerClass(Reduce_First.class); 

    //job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
    //job1.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value 
  
    job1.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
    job1.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value 
    
    //加入控制容器 
    ControlledJob ctrljob1=new  ControlledJob(conf); 
    ctrljob1.setJob(job1); 
    //job1的输入输出文件路径
    FileInputFormat.addInputPath(job1, new Path(args[0])); 
      FileOutputFormat.setOutputPath(job1, new Path(args[1])); 

      //第二个作业的配置
    	@SuppressWarnings("deprecation")
		Job job2=new Job(conf,"Join2"); 
      job2.setJarByClass(MODEL.class); 
      
      job2.setMapperClass(Map_Second.class); 
     job2.setReducerClass(Reduce_Second.class); 
     
   // job2.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
   // job2.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value 

    job2.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
    job2.setOutputValueClass(Text.class);//reduce阶段的输出的value 

    //作业2加入控制容器 
    ControlledJob ctrljob2=new ControlledJob(conf); 
    ctrljob2.setJob(job2); 
  
     //设置多个作业直接的依赖关系 
       //如下所写： 
     //意思为job2的启动，依赖于job1作业的完成 
  
    ctrljob2.addDependingJob(ctrljob1); 
    
    //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    
    //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
    //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
    FileOutputFormat.setOutputPath(job2,new Path(args[2]) );

    //主的控制容器，控制上面的总的两个子作业 
    JobControl jobCtrl=new JobControl("myctrl"); 
  
    //添加到总的JobControl里，进行控制
    jobCtrl.addJob(ctrljob1); 
    jobCtrl.addJob(ctrljob2); 


    //在线程启动，记住一定要有这个
    Thread  t=new Thread(jobCtrl); 
    t.start(); 

    while(true){ 

    if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息 
    System.out.println(jobCtrl.getSuccessfulJobList()); 
    jobCtrl.stop(); 
    break; 
    }
    }
    }
}
