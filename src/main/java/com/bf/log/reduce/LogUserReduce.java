package com.bf.log.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.bf.log.dimention.UserDimention;

public class LogUserReduce extends Reducer<UserDimention, IntWritable, UserDimention, IntWritable> {
	@Override
	protected void reduce(UserDimention arg0, Iterable<IntWritable> arg1,
			Reducer<UserDimention, IntWritable, UserDimention, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		
		int count=0;
		if(arg0.getLogEN().equals("en=e_l")){
			for (IntWritable intWritable : arg1) {
				count++;
			}
			
		}
		
		
		arg2.write(arg0, new IntWritable(count));
		
	}
}
