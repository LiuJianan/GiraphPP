package com.ibm.giraph.graph.example.mapred;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;

import org.apache.giraph.graph.LongLongLongNeighborhood;
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

public class TextBinaryInOutEdgeMerge {
    public static class SMapper extends
	    Mapper<LongWritable, Text, LongWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

	    String[] tokens = value.toString().split("\\s+");
	    StringBuilder sb = new StringBuilder();
	    sb.append("#");
	    for (int i = 2; i < tokens.length; i++) {
		context.write(new LongWritable(Long.parseLong(tokens[i])),
			new Text(tokens[0])); // in edge
		sb.append(" ");
		sb.append(tokens[i]);
	    }
	    context.write(new LongWritable(Long.parseLong(tokens[0])),
		    new Text(sb.toString()));

	}
    }

    public static class SReducer extends
	    Reducer<LongWritable, Text, LongWritable, LongLongLongNeighborhood> {

	public void reduce(LongWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {

	    LongLongLongNeighborhood val = new LongLongLongNeighborhood();

	    HashSet<Long> inedges = new HashSet<Long>();
	    HashSet<Long> outedges = new HashSet<Long>();
	    
	    
	    Iterator<Text> it = values.iterator();

	    while (it.hasNext()) {
		
		String str = it.next().toString();
		if (str.startsWith("#")) {
		    String[] nbs = str.substring(2).split("\\s+");
		    for (int j = 0; j < nbs.length; j++) {
			inedges.add(Long.parseLong(nbs[j]));
		    }
		} else {
		    outedges.add(Long.parseLong(str));
		}
	    }
	    
	    Vector<Long> edges = new Vector<Long>();
	    Vector<Long> edgeValues = new Vector<Long>();
	    
	    Iterator<Long> in = inedges.iterator();
	    while(in.hasNext())
	    {
		Long e = in.next();
		edges.add(e);
		if(outedges.contains(e))
		{
		    edgeValues.add(2L); // both
		}
		else
		{
		    edgeValues.add(1L);
		}
	    }
	    
	    Iterator<Long> out = outedges.iterator();
	    
	    while(out.hasNext())
	    {
		Long e = out.next();
		if(inedges.contains(e))
		    continue;
		edges.add(e);
		edgeValues.add(0L);
	    }
	    
	    val.setSimpleEdges(edges, edgeValues);

	    context.write(key, val);

	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = new Job(conf, "TextBinaryInOutEdgeMerge");
	String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
	if (otherArgs.length != 3) {
	    System.err.println("Args: <in> <out> <num>");
	    System.exit(3);
	}

	job.setJarByClass(TextBinaryInOutEdgeMerge.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setMapperClass(SMapper.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setMapOutputKeyClass(LongWritable.class);// cnt
	job.setMapOutputValueClass(Text.class);// vid

	job.setReducerClass(SReducer.class);

	job.setOutputFormatClass(KVBinaryOutputFormat.class);
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(LongLongLongNeighborhood.class);

	job.setNumReduceTasks(Integer.parseInt(args[2]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}