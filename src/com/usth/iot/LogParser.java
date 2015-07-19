package com.usth.iot;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 27.19.74.143 - - [30/May/2013:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
 */

public class LogParser {
	
	public String[] parse(String line){
		String ip = parseIp(line);
		String date = parseDate(line);
		String url = parseUrl(line);
		String status = parseStatus(line);
		String traffic = parseTraffic(line);
		return new String[]{ip,date,url,status,traffic};
	}
	
	private String parseTraffic(String line) {
		String[] split = line.split("\"");
		String traffic = split[2].split("\\s")[2];
		return traffic;
	}

	private String parseStatus(String line) {
		String[] split = line.split("\"");
		String status = split[2].split("\\s")[1];
		return status;
	}

	private String parseUrl(String line) {
		String[] split = line.split("\"");
		String url = split[1];
		return url;
	}

	private String parseDate(String line) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyyMMddHHmmss");
		String date = ((line.split("]")[0].split("-"))[2].split("\\s"))[1].substring(1);
		Date parse = null;
		try {
			parse = simpleDateFormat.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String format = simpleDateFormat2.format(parse);
		return format;
	}
	
	private String parseIp(String line) {
		String[] split = line.split("-");
		String ip = split[0].trim();
		return ip;
	}
}
