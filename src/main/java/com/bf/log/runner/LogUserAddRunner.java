package com.bf.log.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bf.log.dimention.UserDimention;
import com.bf.log.format.LogUserFormat;
import com.bf.log.mapper.LogUserAnalyzeMapper;
import com.bf.log.reduce.LogUserReduce;

public class LogUserAddRunner implements Tool {
	private Configuration conf;

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		conf.addResource("jdbc_cfg.xml");
		conf.addResource("sql_mapper.xml");
		this.conf=conf;

	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(conf);
		job.setJarByClass(LogUserAddRunner.class);
		
		job.setMapperClass(LogUserAnalyzeMapper.class);
		
		job.setMapOutputKeyClass(UserDimention.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(LogUserReduce.class);
		
		job.setOutputKeyClass(UserDimention.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/bmdout/part-r-00000"));
		//FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun2:9000/bmdout"));
		job.setOutputFormatClass(LogUserFormat.class);
		
		if(job.waitForCompletion(true)){
			return 1 ;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	 int i=	ToolRunner.run(new LogUserAddRunner(), args);
	 System.out.println(i);

	}

}
