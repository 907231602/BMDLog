package com.bf.log.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserDimention implements WritableComparable<UserDimention>{
	
	private String logDate;
	private String logEN;
	private String logUUD;
	
	/*private String logP_URL;
	private String logU_MID;
	*/

	public String getLogDate() {
		return logDate;
	}

	public void setLogDate(String logDate) {
		this.logDate = logDate;
	}

	public String getLogEN() {
		return logEN;
	}

	public void setLogEN(String logEN) {
		this.logEN = logEN;
	}

	public String getLogUUD() {
		return logUUD;
	}

	public void setLogUUD(String logUUD) {
		this.logUUD = logUUD;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(logDate);
		out.writeUTF(logEN);
		out.writeUTF(logUUD);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
	this.logDate=in.readUTF();
	this.logEN=in.readUTF();
	this.logUUD=in.readUTF();
		
	}

	public int compareTo(UserDimention arg0) {
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.logDate.compareTo(arg0.logDate);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logEN.compareTo(arg0.logEN);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.logUUD.compareTo(arg0.logUUD);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return logDate+"\t"+logEN+"\t"+logUUD;
	}
	
	
		
}
