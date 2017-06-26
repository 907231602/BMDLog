package com.bf.log.dimention;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserValueDimention implements Writable {

	private String log_UUD;
	private String total;
	
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

}
