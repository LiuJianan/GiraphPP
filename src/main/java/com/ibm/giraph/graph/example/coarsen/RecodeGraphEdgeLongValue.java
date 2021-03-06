package com.ibm.giraph.graph.example.coarsen;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.giraph.graph.LongLongLongNeighborhood;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenLongLongHashMap;
import org.mortbay.log.Log;

import com.ibm.giraph.graph.example.ioformats.KVBinaryInputFormat;
import com.ibm.giraph.graph.example.ioformats.KVBinaryOutputFormat;


public class RecodeGraphEdgeLongValue implements Tool {

	private Configuration conf;
	public static final String MAP_FILE_NAME="map.file.name";
	public static final String NUM_NODES="num.nodes";

	static class MyMapper
			extends
			Mapper<LongWritable, LongLongLongNeighborhood, 
	LongWritable, LongLongLongNeighborhood>
	{
		private int[] map;
		public void setup(Context context) throws IOException, InterruptedException 
		{
			long n=context.getConfiguration().getLong(NUM_NODES, 0);
			map=new int[(int) n+1];
			JobConf conf=new JobConf(context.getConfiguration());
			try {
				long newid=0;
				Path[] files=DistributedCache.getLocalCacheFiles(conf);
				TreeMap<String, Path> ordered=new TreeMap<String, Path>();
				for(Path file: files)
				{
					ordered.put(file.getName(), file);
				}
				/////////////////
				for(Path file: ordered.values())
				{
					Log.info("load distributed cache: "+file);
					DataInputStream in = new DataInputStream (new BufferedInputStream(new FileInputStream(file.toString())));
					while(true)
					{
						try{
							long oldid=in.readLong();
							map[(int)oldid]=(int)newid;
							newid++;
						}catch (EOFException e)
						{
							break;
						}catch (IOException e)
						{
							throw e;
						}
					}
					in.close();
				}
			}catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		public void map(LongWritable key, LongLongLongNeighborhood value, Context context)
		throws IOException, InterruptedException 
		{
			key.set(map[(int) key.get()]);
			for(int i=0; i<value.getNumberEdges(); i++)
			{
				value.setEdgeID(i, map[((int) value.getEdgeID(i))]);
			}
			context.write(key, value);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("At least 3 arguments are requiered: <graph file> <recode mapping> <OutputPath> <#nodes>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJobName(this.getClass().getName());
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongLongLongNeighborhood.class);
		job.setInputFormatClass(KVBinaryInputFormat.class);
		KVBinaryInputFormat.setInputNeighborhoodClass(job.getConfiguration(), LongLongLongNeighborhood.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set(MAP_FILE_NAME, args[1]);
		Path outpath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outpath);
		job.setJarByClass(RecodeGraphEdgeLongValue.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(KVBinaryOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongLongLongNeighborhood.class);
		
		JobConf conf=new JobConf(job.getConfiguration());
		FileSystem fs=FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(args[1]));
		for(FileStatus stat: stats)
			if(stat.getPath().getName().contains("part"))
				DistributedCache.addCacheFile(stat.getPath().toUri(), conf);
		job=new Job(conf);
		
		if (FileSystem.get(job.getConfiguration()).exists(outpath)) {
			FileSystem.get(job.getConfiguration()).delete(outpath, true);
		}
		job.getConfiguration().setLong(NUM_NODES, Long.parseLong(args[3]));
		job.waitForCompletion(true);
		return 0;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration cf) {
		conf=cf;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new RecodeGraphEdgeLongValue(), args);
	}
}
