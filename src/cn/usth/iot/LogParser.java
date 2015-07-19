package cn.usth.iot;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 27.19.74.143 - - [30/May/2013:17:38:20 +0800]
 * "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
 * 
 * @author martin
 *
 */
public class LogParser {

	public String[] parse(String line) {
		String ip = parseIP(line);
		String date = parseTime(line);
		String url = parseUrl(line);
		String status = parseStatus(line);
		String traffic = parseTraffic(line);
		return new String[] { ip, date, url, status, traffic };
	}

	private String parseIP(String line) {
		int index = line.indexOf(" - - ");
		String ip = line.substring(0, index);
		return ip;
	}

	private String parseTime(String line) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		int first = line.indexOf("[");
		int last = line.indexOf(" +0800]");
		String time = line.substring(first + 1, last);
		Date parseDate = parseDate(time);
		String date = dateFormat.format(parseDate);
		return date;
	}

	private String parseUrl(String line) {
		int first = line.indexOf("\"");
		int last = line.lastIndexOf("\"");
		String url = line.substring(first + 1, last);
		return url;
	}

	private String parseStatus(String line) {
		int first = line.lastIndexOf("\"");
		String substring = line.substring(first + 2);
		String status = substring.split(" ")[0];
		return status;
	}

	private String parseTraffic(String line) {
		int first = line.lastIndexOf("\"");
		String substring = line.substring(first + 2);
		String traffic = substring.split(" ")[1];
		return traffic;
	}

	private Date parseDate(String date) {
		Date parse = null;
		SimpleDateFormat dateFormat1 = new SimpleDateFormat(
				"dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		try {
			parse = dateFormat1.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return parse;
	}
}
