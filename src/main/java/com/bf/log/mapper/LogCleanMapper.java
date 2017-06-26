package com.bf.log.mapper;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bf.log.constants.ToDate;


public class LogCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.map(key, value, context);\\
		String[] values=value.toString().split(" ");
		
		String logIp="-";
		String logDate="-";
		String logMethod="-";
		String logUrl="-";
		String logStatus="-";
		
		if(values.length>0){
			logIp=values[0];
		}if(values.length>5){
			//System.out.println(values[3]+" "+values[4]);
			logDate=ToDate.toDate(values[3]+" "+values[4]);
			//System.out.println(logDate);
		}
		if(values.length>6){
			logMethod=values[5];
		}if(values.length>7){
			logUrl=URLDecoder.decode(values[6]) ;
		}
		if(values.length>9){
			logStatus=values[8];
		}
		
		if(logUrl.contains("?")){
			
			context.write(new Text(logIp+"\t"+logDate+"\t"+logMethod+"\t"+logUrl+"\t"+logStatus), NullWritable.get());
		}
		
		
		
	}



}
