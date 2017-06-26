package com.bf.log.test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String str="192.168.0.123 - - [19/Jun/2017:08:17:34 +0800] GET /HHUspringmvc/aa.xhtml?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2FBIG_DATA_LOG2%2Fdemo.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A21&ver=1&pl=website&sdk=js&u_ud=550DE3C3-3923-4C36-BDBD-45784A6E314E&u_mid=yuhui&u_sd=C4BB84B4-B155-4A64-BE11-553DE5E56490&c_time=1497831453082&b_iev=Mozilla%2F5.0%20(compatible%3B%20MSIE%2010.0%3B%20Windows%20NT%206.1%3B%20WOW64%3B%20Trident%2F7.0%3B%20LCTE)&b_rst=1366*768 HTTP/1.1 404 949";
		String[] tt=str.split(" ");
		for (String string : tt) {
			System.out.println(string);
		}
		toDate("[19/Jun/2017:08:17:34 +0800]");
	}

	public static String toDate(String date){
		
		SimpleDateFormat in = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss ZZZZZ]", Locale.US);
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Date date1 = in.parse(date);
			date = out.format(date1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(date);
		return date;
	}
}
