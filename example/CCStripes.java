import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CCStripes {
  public static class StripesMapper 
      extends Mapper<Object, Text, Text, MapWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word1 = new Text();
     
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr1 = new StringTokenizer(value.toString());
      while (itr1.hasMoreTokens()) {	  
	  word1.set(itr1.nextToken().toLowerCase());
	  MapWritable mymap = new MapWritable();
	  StringTokenizer itr2 = new StringTokenizer(value.toString());
	  while (itr2.hasMoreTokens()) {
	      Text word2 = new Text(itr2.nextToken().toLowerCase());
	      if (word1.compareTo(word2) < 0) {
		  IntWritable count = (IntWritable)mymap.get(word2);
		  if (count == null)
		      count = one;
		  else
		      count = new IntWritable(count.get() + 1);
		  mymap.put(word2, count);
	      }
	  }
	  if (!mymap.isEmpty()) {
	      context.write(word1, mymap);
	  }
      }
    }
  }
  
  public static class StripesCombiner
      extends Reducer<Text, MapWritable, Text, MapWritable> {

      private Text pair = new Text();

    public void reduce(Text word1, Iterable<MapWritable> stripes, 
                       Context context
                       ) throws IOException, InterruptedException {
	MapWritable map = new MapWritable();
	for (MapWritable stripe: stripes) {
	    for (Map.Entry<Writable,Writable> entry: stripe.entrySet()) {
		Text word2 = (Text)entry.getKey();
		IntWritable newCount = (IntWritable)entry.getValue();
		IntWritable count = (IntWritable)map.get(word2);
		if (count == null)
		    count = newCount;
		else
		    count = new IntWritable(count.get() + newCount.get());
		map.put(word2, count);
	    }
	}
	context.write(word1, map);
    }
  }

  public static class StripesReducer
      extends Reducer<Text, MapWritable, Text, IntWritable> {

      private Text pair = new Text();

    public void reduce(Text word1, Iterable<MapWritable> stripes, 
                       Context context
                       ) throws IOException, InterruptedException {
	MapWritable map = new MapWritable();
	for (MapWritable stripe: stripes) {
	    for (Map.Entry<Writable,Writable> entry: stripe.entrySet()) {
		Text word2 = (Text)entry.getKey();
		IntWritable newCount = (IntWritable)entry.getValue();
		IntWritable count = (IntWritable)map.get(word2);
		if (count == null)
		    count = newCount;
		else
		    count = new IntWritable(count.get() + newCount.get());
		map.put(word2, count);
	    }
	}
	for (Map.Entry<Writable,Writable> entry: map.entrySet()) {
	    Text word2 = (Text)entry.getKey();
	    pair.set(word1.toString() + ":" + word2.toString());
	    context.write(pair, (IntWritable)entry.getValue());
	}
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
    Job job = new Job(conf, "Cross-correlation using stripes");
    job.setJarByClass(CCStripes.class);
    job.setMapperClass(StripesMapper.class);
    job.setCombinerClass(StripesCombiner.class);
    job.setReducerClass(StripesReducer.class);
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
