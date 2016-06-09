/*
  Code based on https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

package ece454;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopWC {
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, ArrayWritable>{
    
    //private final static ArrayWritable arr = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	StringTokenizer itr = new StringTokenizer(value.toString(), ",");
	ArrayWritable arr = new ArrayWritable();
	IntWritable[] intArr = new IntWritable[7];
	int cnt=0;
      while (itr.hasMoreTokens()) {
		if(cnt==0){
			word.set(itr.nextToken());
			intArr[cnt] = new IntWritable(0);
		}
        else{
			intArr[cnt]=(IntWritable)itr.nextToken();
		}
		if(cnt==7){
			cnt =0;
			context.write(word, new ArrayWritable(intArr));
		}
		else{cnt++;}       
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,ArrayWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(HadoopWC.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
