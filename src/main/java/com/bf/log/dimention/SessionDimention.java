package com.bf.log.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SessionDimention implements WritableComparable<SessionDimention> {
	
	private String sessionUSD;
	private String sessionNumber;//会话个数
	private String sessionLength;//会话长度
	private String sessionMax;
	private String sessionMin;
	private String sessionShort;//差值
	

	public String getSessionMax() {
		return sessionMax;
	}

	public void setSessionMax(String sessionMax) {
		this.sessionMax = sessionMax;
	}

	public String getSessionMin() {
		return sessionMin;
	}

	public void setSessionMin(String sessionMin) {
		this.sessionMin = sessionMin;
	}

	public String getSessionShort() {
		return sessionShort;
	}

	public void setSessionShort(String sessionShort) {
		this.sessionShort = sessionShort;
	}

	public String getSessionUSD() {
		return sessionUSD;
	}

	public void setSessionUSD(String sessionUSD) {
		this.sessionUSD = sessionUSD;
	}

	public String getSessionNumber() {
		return sessionNumber;
	}

	public void setSessionNumber(String sessionNumber) {
		this.sessionNumber = sessionNumber;
	}

	public String getSessionLength() {
		return sessionLength;
	}

	public void setSessionLength(String sessionLength) {
		this.sessionLength = sessionLength;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(sessionUSD);
		out.writeUTF(sessionNumber);
		out.writeUTF(sessionLength);
		out.writeUTF(sessionMax);
		out.writeUTF(sessionMin);
		out.writeUTF(sessionShort);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.sessionUSD= in.readUTF();
		this.sessionNumber=in.readUTF();
		this.sessionLength=in.readUTF();
		this.sessionMax=in.readUTF();
		this.sessionMin=in.readUTF();
		this.sessionShort=in.readUTF();
		
	}

	public int compareTo(SessionDimention arg0) {
		// TODO Auto-generated method stub
		if(this==arg0){
			return 0;
		}
		int tmp=this.sessionUSD.compareTo(arg0.sessionUSD);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.sessionNumber.compareTo(arg0.sessionNumber);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.sessionLength.compareTo(arg0.sessionLength);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.sessionMax.compareTo(arg0.sessionMax);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.sessionMin.compareTo(arg0.sessionMin);
		if (tmp!=0) {
			return tmp;
		}
		tmp=this.sessionShort.compareTo(arg0.sessionShort);
		if (tmp!=0) {
			return tmp;
		}
		return 0;
	}

	@Override
	public String toString() {
		return "SessionDimention [sessionUSD=" + sessionUSD + ", sessionNumber=" + sessionNumber + ", sessionLength="
				+ sessionLength + ", sessionMax=" + sessionMax + ", sessionMin=" + sessionMin + ", sessionShort="
				+ sessionShort + "]";
	}


}
