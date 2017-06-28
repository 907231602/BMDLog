package com.bf.log.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bf.log.constants.ToDate;
import com.bf.log.dimention.SessionDimention;
import com.bf.log.dimention.UserDimention;
import com.bf.log.dimention.UserMemberDimention;

public class LogUserAnalyzeMapper extends Mapper<LongWritable, Text, UserDimention, LongWritable> {

	private int count = 0;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");

		String logDate = ToDate.toHouse(values[1]);
		String logUrl = values[3];
		String[] logValue = logUrl.split("&");
		
		UserMemberDimention userMemberDimention = new UserMemberDimention();
		userMemberDimention.setLogP_URL("-");
		userMemberDimention.setLogU_MID("-");
		
		SessionDimention sessionDimention=new SessionDimention();
		sessionDimention.setSessionUSD("-");
		sessionDimention.setSessionNumber("-");
		sessionDimention.setSessionLength("-");
		sessionDimention.setSessionMax("-");
		sessionDimention.setSessionMin("-");
		sessionDimention.setSessionShort("-");
		

		/////////////////////////// 新增用户////////////////////////

		if (logUrl.contains("en=e_l")) {
			
			UserDimention userDimention = new UserDimention();
			userDimention.setLogDate(logDate);
			userDimention.setLogEN("en=e_l");
			userDimention.setLogUUD("-");
			userDimention.setUserMemberDimention(userMemberDimention);
			userDimention.setSessionDimention(sessionDimention);
			context.write(userDimention, new LongWritable(1));
		}

		/////////////////////////// 活跃用户///////////////////////
		
		for (String string : logValue) {
			if (string.contains("u_ud")) {
				String[] logUUD = string.split("=");
				UserDimention userDimention = new UserDimention();
				userDimention.setLogDate(logDate);
				userDimention.setLogEN("u_ud");
				userDimention.setLogUUD(logUUD[1]);
				userDimention.setUserMemberDimention(userMemberDimention);
				userDimention.setSessionDimention(sessionDimention);
				context.write(userDimention, new LongWritable(1));
				//System.out.println(string);
				break;
			}
		}

		////////////////////// 新增会员-1/////////////////////////////
		/*if (logUrl.contains("demo4.jsp")) {
			for (String string : logValue) {
				if (string.contains("u_mid")) {
					String[] logUMID = string.split("=");
					UserDimention userDimention = new UserDimention();
					userDimention.setLogDate(logDate);
					userDimention.setLogEN("-");
					userDimention.setLogUUD("-");
					userMemberDimention.setLogP_URL("p_url");// 用于判别新增会员的类型
					userMemberDimention.setLogU_MID(logUMID[1]);
					userDimention.setUserMemberDimention(userMemberDimention);
					context.write(userDimention, new LongWritable(1));
					break;
				}
			}
		}*/
			//////////////////////新增会员-2/////////////////////////////
		for (String strings : logValue) {
			if(strings.contains("demo4.jsp") && strings.contains("p_url")){
				for (String string : logValue) {
					if (string.contains("u_mid")) {
						String[] logUMID = string.split("=");
						UserDimention userDimention = new UserDimention();
						userDimention.setLogDate(logDate);
						userDimention.setLogEN("-");
						userDimention.setLogUUD("-");
						userMemberDimention.setLogP_URL("p_url");// 用于判别新增会员的类型
						//userMemberDimention.setLogU_MID(logUMID[1]);
						userMemberDimention.setLogU_MID("-");
						userDimention.setUserMemberDimention(userMemberDimention);
						userDimention.setSessionDimention(sessionDimention);
						context.write(userDimention, new LongWritable(1));
						break;
					}
				}
				break;
			}
		}
		
		

		///////////////////// 活跃会员///////////////////////////////////

		if (logUrl.contains("u_mid")) {
			UserDimention userDimention = new UserDimention();
			userDimention.setLogDate(logDate);
			userDimention.setLogEN("-");
			userDimention.setLogUUD("-");
			
			userMemberDimention.setLogP_URL("p_UMID");// 用于判别活跃会员的类型
			for (String string : logValue) {
				if (string.contains("u_mid"))
					userMemberDimention.setLogU_MID(string);
				//break;
			}
			userDimention.setUserMemberDimention(userMemberDimention);
			userDimention.setSessionDimention(sessionDimention);
			context.write(userDimention, new LongWritable(1));
		}
		
		
		
		/////////////////////////////会话个数/////////////////////////////////////////////
		for(String val:logValue){
			if(val.contains("u_sd")){
				UserDimention userDimention=new UserDimention();
				userDimention.setLogDate(logDate);
				userDimention.setLogEN("-");
				userDimention.setLogUUD("-");
				userMemberDimention.setLogP_URL("-");
				userMemberDimention.setLogU_MID("-");
				userDimention.setUserMemberDimention(userMemberDimention);
				sessionDimention.setSessionNumber("sessionNumber");
				sessionDimention.setSessionUSD(val);
				userDimention.setSessionDimention(sessionDimention);
				context.write(userDimention, new LongWritable(1));
			}
		}
		
		////////////////////////会话长度////////////////////////////////////////////////////////
		for(String val:logValue){
			if(val.contains("u_sd")){
				UserDimention userDimention=new UserDimention();
				userDimention.setLogDate(logDate);
				userDimention.setLogEN("-");
				userDimention.setLogUUD("-");
				userMemberDimention.setLogP_URL("-");
				userMemberDimention.setLogU_MID("-");
				userDimention.setUserMemberDimention(userMemberDimention);
				sessionDimention.setSessionNumber("-");
				sessionDimention.setSessionLength("sessionLength");
				sessionDimention.setSessionUSD(val);
				userDimention.setSessionDimention(sessionDimention);
				long c_time=0L;
				for (String string : logValue) {
					if(string.contains("c_time")){
						String[] vv=string.split("=");
						c_time=Long.parseLong(vv[1]);
						break;
					}
				}
				
				context.write(userDimention, new LongWritable(c_time));
			}
		}
		
		
		

	}
}
