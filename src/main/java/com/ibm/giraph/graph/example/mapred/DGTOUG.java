package com.ibm.giraph.graph.example.mapred;

import java.io.*;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class DGTOUG {
	public static class SMapper extends
			Mapper<LongWritable, Text, LongWritable, LongWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String tokens [] = value.toString().split("\\s+");
			
			for(int i = 2; i < tokens.length ; i ++)
			{
			    context.write(new LongWritable(Long.parseLong(tokens[0])), new LongWritable(Long.parseLong(tokens[i])));
			    context.write(new LongWritable(Long.parseLong(tokens[i])), new LongWritable(Long.parseLong(tokens[0])));
			}
			

			context.write(new LongWritable(Long.parseLong(tokens[0])), new LongWritable(-1));
			
			
		}
	}

	public static class SReducer extends
			Reducer<LongWritable, LongWritable, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

		    	SortedSet<Long> nbs = new TreeSet<Long>();
		    	Iterator<LongWritable> it = values.iterator();
		    	while(it.hasNext())
		    	{
		    	    long v = it.next().get();
		    	    if(v!= key.get() && v != -1) 
		    		nbs.add(v);
		    	}
		    	
		    	StringBuilder sb = new StringBuilder();
		    	sb.append(nbs.size());
		    	for(long v : nbs)
		    	{
		    	    sb.append(" ");
		    	    sb.append(v);
		    	}
		    	
		    	context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DGTOUG");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Args: <in> <out>");
			System.exit(2);
		}

		job.setJarByClass(DGTOUG.class);

		job.setMapperClass(SMapper.class);
		job.setReducerClass(SReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(60);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
