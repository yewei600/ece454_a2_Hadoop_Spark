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

public class CCPairs {
  public static class PairsMapper 
      extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word1 = new Text();
    private Text word2 = new Text();
    private Text pair = new Text();
     
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr1 = new StringTokenizer(value.toString());
      while (itr1.hasMoreTokens()) {
	  word1.set(itr1.nextToken().toLowerCase());
	  StringTokenizer itr2 = new StringTokenizer(value.toString());
	  while (itr2.hasMoreTokens()) {
	      word2.set(itr2.nextToken().toLowerCase());
	      if (word1.compareTo(word2) < 0) {
		  pair.set(word1.toString() + ":" + word2.toString());
		  context.write(pair, one);
	      }
	  }	  
      }
    }
  }
  
  public static class PairsReducer 
      extends Reducer<Text, IntWritable, Text, IntWritable> {

      private IntWritable result = new IntWritable();
      private Text pair = new Text();

    public void reduce(Text pair, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(pair, result);
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
    Job job = new Job(conf, "Cross-correlation using pairs");
    job.setJarByClass(CCPairs.class);
    job.setMapperClass(PairsMapper.class);
    job.setCombinerClass(PairsReducer.class);
    job.setReducerClass(PairsReducer.class);
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
