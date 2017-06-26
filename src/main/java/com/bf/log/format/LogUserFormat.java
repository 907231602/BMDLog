package com.bf.log.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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


public class LogUserFormat extends OutputFormat<UserDimention, IntWritable> {

	@Override
	public RecordWriter<UserDimention, IntWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=context.getConfiguration();
		
		Connection connection=JdbcManager.getConnection(conf);
		
		return new MyUserRecordWriter(conf, connection);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),context);
	}
	
	class MyUserRecordWriter extends RecordWriter<UserDimention, IntWritable>{
		  private Configuration conf;
    	  private Connection con;
		
		public MyUserRecordWriter(Configuration conf,Connection con) {
			// TODO Auto-generated constructor stub
			this.conf=conf;
			this.con=con;
		}
		
		HashMap<String, Integer> hashAdd=new HashMap<String, Integer>();
		LinkedHashMap<String, Integer> hashVisit=new LinkedHashMap<String, Integer>();
		  private HashMap<String,PreparedStatement> psMaps=new HashMap<String,PreparedStatement>();
		@Override
		public void write(UserDimention key, IntWritable value) throws IOException, InterruptedException {
			if(key.getLogEN().equals("en=e_l")){
				//int	addUserCount=((hashAdd.get(key.getLogDate())==null?0:(int)hashAdd.get(key.getLogDate()))+1);
				
				hashAdd.put(key.getLogDate(), value.get());
				//System.out.println(hashAdd.get(key.getLogDate()));
			}else{
				int	visitUserCount=((hashVisit.get(key.getLogDate())==null?0:(int)hashVisit.get(key.getLogDate()))+1);
				hashVisit.put(key.getLogDate(), visitUserCount);
			}
			
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			PreparedStatement ps=psMaps.get("user_add");
			if (ps == null) {
				try {
					ps=con.prepareStatement(conf.get("user_add"));
					psMaps.put("user_add", ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		for(String kk:hashAdd.keySet()){
			try {
				ps.setString(1,kk);
				ps.setString(2,Integer.toString(hashAdd.get(kk)));
				
				ps.setString(3,Integer.toString(hashAdd.get(kk)));
				ps.execute();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		ps=psMaps.get("user_visit");
		if (ps == null) {
			try {
				ps=con.prepareStatement(conf.get("user_visit"));
				psMaps.put("user_visit", ps);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/*for(String hh:hashVisit.keySet()){
			try {
				ps.setString(1, hh);
				ps.setString(2, Integer.toString(hashVisit.get(hh)) );
				
				ps.setString(3, Integer.toString(hashVisit.get(hh)) );
				ps.execute();
				
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}*/
		
		//int count=0;
		int he=0;
		for(String hh:hashVisit.keySet()){
			try {
				//count++;
				//if(count>1){
					//System.out.println(hh+";"+(he+hashVisit.get(hh)));
					//he=he+hashVisit.get(hh);
					ps.setString(1, hh);
					ps.setString(2, Integer.toString(hashVisit.get(hh)) );
					ps.setString(3, Integer.toString(he+hashVisit.get(hh)) );
					
					ps.setString(4, Integer.toString(hashVisit.get(hh)) );
					ps.setString(5, Integer.toString(he+hashVisit.get(hh)) );
					he=he+hashVisit.get(hh);
					ps.execute();
				//}
				/*else{
					ps.setString(1, hh);
					ps.setString(2, Integer.toString(hashVisit.get(hh)) );
					ps.setString(3, Integer.toString(he+hashVisit.get(hh)) );
					
					ps.setString(4, Integer.toString(hashVisit.get(hh)) );
					ps.setString(5, Integer.toString(he+hashVisit.get(hh)) );
					he=he+hashVisit.get(hh);
					ps.execute();
				}*/
				
				
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
			
		
			try {
				ps.close();
				con.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
			
			
			
		}
		
	} 
	
	

}
