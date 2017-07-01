package com.bf.log.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserMemberDimention  implements WritableComparable<UserMemberDimention>{
	
	private String logP_URL;
	private String logU_MID;
	
	
	public String getLogP_URL() {
		return logP_URL;
	}
	public void setLogP_URL(String logP_URL) {
		this.logP_URL = logP_URL;
	}
	public String getLogU_MID() {
		return logU_MID;
	}
	public void setLogU_MID(String logU_MID) {
		this.logU_MID = logU_MID;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(logP_URL);
		out.writeUTF(logU_MID);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		logP_URL= in.readUTF();
		logU_MID=in.readUTF();
	}
	
	public int compareTo(UserMemberDimention arg0) {
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.logP_URL.compareTo(arg0.logP_URL);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logU_MID.compareTo(arg0.logU_MID);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return logP_URL+"\t"+logU_MID;
	}
	
}
