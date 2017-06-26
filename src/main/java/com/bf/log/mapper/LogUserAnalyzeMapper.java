package com.bf.log.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bf.log.constants.ToDate;
import com.bf.log.dimention.UserDimention;

public class LogUserAnalyzeMapper extends Mapper<LongWritable,Text, UserDimention, IntWritable> {

	private int count=0;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		String[] values=value.toString().split("\t");
		
		 String logDate= ToDate.toHouse(values[1]) ;
		 String logUrl=values[3];
		 
		 ///////////////////////////新增用户////////////////////////
		 
		 if(logUrl.contains("en=e_l")){
			// count++;
			// System.out.println(count);
			 UserDimention userDimention= new UserDimention();
			 userDimention.setLogDate(logDate);
			 userDimention.setLogEN("en=e_l");
			 userDimention.setLogUUD("1");
			 context.write(userDimention, new IntWritable(1));
		 }
		 
		 
		 ///////////////////////////活跃用户///////////////////////
		 String[] logValue=logUrl.split("&");
		 for (String string : logValue) {
			if(string.contains("u_ud")){
				String[] logUUD=string.split("=");
				 UserDimention userDimention= new UserDimention();
				 userDimention.setLogDate(logDate);
				 userDimention.setLogEN("1");
				 userDimention.setLogUUD(logUUD[1]);
				
				context.write(userDimention, new IntWritable(1));
				break;
			}
		}
		 
		 
		 
		 
		
		
		
	}
}
