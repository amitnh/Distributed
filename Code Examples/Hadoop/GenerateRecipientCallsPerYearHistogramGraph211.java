package dsp.hadoop.examples;

import java.io.IOException;


import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dsp.hadoop.examples.Action211.ActionType;
  
public class GenerateRecipientCallsPerYearHistogramGraph211 { 
 
public static class MapperClass extends Mapper<Text,IntWritable,IntWritable,IntWritable> {
    
	
    @Override
    public void setup(Context context)  throws IOException, InterruptedException {
    }

    @Override
    public void map(Text recipientYear, IntWritable callsCount, Context context) throws IOException,  InterruptedException {
    	context.write(callsCount, new IntWritable(1));
    }
    
    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {
    }

  }
 
  public static class ReducerClass extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(IntWritable callsCountForYear, Iterable<IntWritable> counts, Context context) throws IOException,  InterruptedException {
	    	int sum = 0;
	    	for (IntWritable cont : counts)
	    		sum++;
	    	context.write(callsCountForYear, new IntWritable(sum));
	    }
	    
	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }
 
  /*
  public static class CombinerClass 
     extends Reducer<K2,V2,K3,V3> {

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
	    }
	    
	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }

  public static class PartitionerClass extends Partitioner<K2,V2> {
	  
      @Override
      public int getPartition(K2 word, V2 count, int numReducers) {
        return key.hashCode() % numReducers;
      }
    
    }
 */
  
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
    conf.set("mapred.max.split.size", "1024000");
    Job job = new Job(conf, "Template");
    job.setJarByClass(TMP.class);
    
    job.setMapperClass(MapperClass.class);
    //job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(CombinerClass.class);
    job.setReducerClass(ReducerClass.class);
    
    //job.setMapOutputKeyClass(K2.class);
    //job.setMapOutputValueClass(K2.class);
    //job.setOutputKeyClass(K3.class);
    //job.setOutputValueClass(K3.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(XXX.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
 
}