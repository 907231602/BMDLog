package com.bf.log.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.log.connection.JdbcManager;
import com.bf.log.dimention.UserDimention;

public class LogUserFormat extends OutputFormat<UserDimention, LongWritable> {

	@Override
	public RecordWriter<UserDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();

		Connection connection = JdbcManager.getConnection(conf);

		return new MyUserRecordWriter(conf, connection);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

	class MyUserRecordWriter extends RecordWriter<UserDimention, LongWritable> {
		private Configuration conf;
		private Connection con;

		public MyUserRecordWriter(Configuration conf, Connection con) {
			// TODO Auto-generated constructor stub
			this.conf = conf;
			this.con = con;
		}

		HashMap<String, Integer> hashAdd = new HashMap<String, Integer>();
		LinkedHashMap<String, Integer> hashVisit = new LinkedHashMap<String, Integer>();
		private HashMap<String, PreparedStatement> psMaps = new HashMap<String, PreparedStatement>();
		LinkedHashMap<String, Integer> hashMemberAdd = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> hashMemberVisit = new LinkedHashMap<String, Integer>();
		HashMap<String,Integer> hashSessionNumber=new HashMap<String, Integer>();
		HashMap<String,Long> hashSessionLength=new HashMap<String,Long>();

		@Override
		public void write(UserDimention key, LongWritable value) throws IOException, InterruptedException {
			if (key.getLogEN().equals("en=e_l")) {
				hashAdd.put(key.getLogDate(), (int) value.get());
			} else if (key.getLogEN().equals("u_ud")) {
				int visitUserCount = (hashVisit.containsKey(key.getLogDate())
						? hashVisit.get(key.getLogDate()): 0);
				hashVisit.put(key.getLogDate(), visitUserCount+1);
			} else if (key.getUserMemberDimention().getLogP_URL().equals("p_url")) {
				/*int addMember = (hashMemberAdd.get(key.getLogDate()) == null ? 0
						: (int) hashMemberAdd.get(key.getLogDate())) + 1;
				hashMemberAdd.put(key.getLogDate(), addMember);*/
				//System.out.println(key.getLogDate()+";;;;;"+ value.get());
				hashMemberAdd.put(key.getLogDate(), (int) value.get());
			} else if (key.getUserMemberDimention().getLogP_URL().equals("p_UMID")) {
				int memberCount = (hashMemberVisit.get(key.getLogDate()) == null ? 0
						: (int) hashMemberVisit.get(key.getLogDate())) + 1;
				hashMemberVisit.put(key.getLogDate(), memberCount);
			}else if(key.getSessionDimention().getSessionNumber().equals("sessionNumber")){
				//hashSessionNumber.put(key.getLogDate(), (int)value.get());
				//System.out.println(key.getLogDate()+"\t"+ (int)value.get());
				int sessionCount = (hashSessionNumber.get(key.getLogDate()) == null ? 0
						: (int) hashSessionNumber.get(key.getLogDate())) + 1;
				hashSessionNumber.put(key.getLogDate(), sessionCount);
			}
			else if(key.getSessionDimention().getSessionLength().equals("sessionLength")){
				long sessionLength = (hashSessionLength.get(key.getLogDate()) == null ? 0
						:  hashSessionLength.get(key.getLogDate())) + Long.parseLong(key.getSessionDimention().getSessionShort()) ;
				hashSessionLength.put(key.getLogDate(), sessionLength);
			}

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {

			////////////////// 对新增用户的写入//////////////////////////////////////////////////////////
			PreparedStatement ps = psMaps.get("user_add");
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get("user_add"));
					psMaps.put("user_add", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (String kk : hashAdd.keySet()) {
				try {
					ps.setString(1, kk);
					ps.setString(2, Integer.toString(hashAdd.get(kk)));

					ps.setString(3, Integer.toString(hashAdd.get(kk)));
					ps.execute();

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			///////////////////////////////// 对活跃用户及每天新增用户累加统计的写入///////////////////////////////////////
			ps = psMaps.get("user_visit");
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get("user_visit"));
					psMaps.put("user_visit", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			int he = 0;
			for (String hh : hashVisit.keySet()) {
				try {

					ps.setString(1, hh);
					ps.setString(2, Integer.toString(hashVisit.get(hh)));
					ps.setString(3, Integer.toString(he + hashVisit.get(hh)));

					ps.setString(4, Integer.toString(hashVisit.get(hh)));
					ps.setString(5, Integer.toString(he + hashVisit.get(hh)));
					he = he + hashVisit.get(hh);
					ps.execute();

				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			//////////////////////////// 新增会员///////////////////////////////////////
			int sum=0;
			/*ps=psMaps.get("member_add");
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get("member_add"));
					psMaps.put("member_add", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			ps=getPSSQL("member_add");
			for (String entry : hashMemberAdd.keySet()) {
				//System.out.println(entry + "===========" + (hashMemberAdd.get(entry)+sum));
				try {
					ps.setString(1, entry);
					ps.setString(2, String.valueOf(hashMemberAdd.get(entry)) );
					ps.setString(3, String.valueOf(hashMemberAdd.get(entry)+sum) );
					
					ps.setString(4, String.valueOf(hashMemberAdd.get(entry)) );
					ps.setString(5, String.valueOf(hashMemberAdd.get(entry)+sum) );
					
					sum=sum+hashMemberAdd.get(entry);
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}

			
			
			
			
			///////////////////////////活跃会员//////////////////////////////////////////
			/*ps=psMaps.get("member_visit");
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get("member_visit"));
					psMaps.put("member_visit", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			ps=getPSSQL("member_visit");
			for (String st : hashMemberVisit.keySet()) {
				try {
					ps.setString(1, st);
					ps.setString(2, Integer.toString(hashMemberVisit.get(st)) );
					ps.setString(3, Integer.toString(hashMemberVisit.get(st)) );
					
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			/////////////////////////////////会话个数/////////////////////////////////////
			ps=getPSSQL("session_add");
			for (String keyy : hashSessionNumber.keySet()) {
				//System.out.println(keyy+"----"+hashSessionNumber.get(keyy));
				try {
					ps.setString(1, keyy);
					ps.setString(2, String.valueOf(hashSessionNumber.get(keyy)) );
					
					ps.setString(3, String.valueOf(hashSessionNumber.get(keyy)) );
					
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//////////////////////////////////////会话长度/////////////////////////////////
			ps=getPSSQL("session_length");
			for (String ee : hashSessionLength.keySet()) {
				try {
					ps.setString(1, ee);
					ps.setString(2, String.valueOf(hashSessionLength.get(ee)) );
					ps.setString(3, String.valueOf(hashSessionLength.get(ee)/hashSessionNumber.get(ee)));
					
					ps.setString(4, String.valueOf(hashSessionLength.get(ee)) );
					ps.setString(5, String.valueOf(hashSessionLength.get(ee)/hashSessionNumber.get(ee)));
					ps.execute();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.println(ee+"\t"+hashSessionLength.get(ee));
			}
			

			
			
			try {
				ps.close();
				con.close();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		////////////////////////获取sql对应的ps语句//////////////////////////////////////
		public PreparedStatement getPSSQL(String sqlName){
			PreparedStatement ps=psMaps.get(sqlName);
			if (ps == null) {
				try {
					ps = con.prepareStatement(conf.get(sqlName));
					psMaps.put(sqlName, ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return ps;
		}

	}
}
