package com.bf.log.constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class ToDate {

		public static String toDate(String date){
			SimpleDateFormat in = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss ZZZZZ]", Locale.US);
			SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd:HH");
			
			try {
				Date date1 = in.parse(date);
				date = out.format(date1);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return date;
		}
		
		public static String toHouse(String date){
				SimpleDateFormat in = new SimpleDateFormat("yyyy-MM-dd:HH");
				SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd");
				
				try {
					Date date1 = in.parse(date);
					date = out.format(date1);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}//System.out.println(date);
				return date;
		}

}
