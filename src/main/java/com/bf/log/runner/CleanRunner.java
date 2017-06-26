package com.bf.log.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bf.log.mapper.LogCleanMapper;


public class CleanRunner implements Tool {
	private Configuration conf;

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf=conf;

	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(conf);
		job.setJarByClass(CleanRunner.class);
		
		job.setMapperClass(LogCleanMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun2:9000/bmd"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun2:9000/bmdout"));
		
		if(job.waitForCompletion(true)){
			return 1 ;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		 int result=ToolRunner.run(new CleanRunner(), args);
		 System.out.println(result);
	}

}
