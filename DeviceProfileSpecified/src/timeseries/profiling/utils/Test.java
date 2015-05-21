package timeseries.profiling.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.io.Text;

import timeseries.profiling.io.DeviceWritable;

public class Test {
	
	public static void main(String[] args) throws ParseException {
		//testLocale();
		//testRecordComplete();
		testDeviceCompare();
		//testTextCompare();
	}
	
	public static void testTextCompare() {
		Text ta = new Text("14GJ123102349");
		Text tb = new Text("13GJ0931001824");
		System.out.println(ta.compareTo(tb));
	}

	public static void testDeviceCompare() {
		DeviceWritable key = new DeviceWritable("14GJ123102349", 204);
		DeviceWritable preKey = new DeviceWritable("13GJ0931001824", 204);
		System.out.println(key.compareTo(preKey));
	}
	
	public static void testLocale() throws ParseException {
		Locale locale = Locale.CHINA;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd ahh:mm:ss",locale);
		System.out.println(df.parse("2015-2-27 上午12:00:00").toString());
	}
	
	public static void testRecordComplete() throws ParseException {
		String[] records = new String[4];
		records[0] = "511500000062,2014-12-19 上午12:00:00,1273.25";
		records[1] = "511500000062,2014-12-20 上午12:00:00,1303.99";
		records[2] = "511500000062,2014-12-22 上午12:00:00,1235.81";
		records[3] = "511500000062,2014-12-24 上午12:00:00,1231.54";
		
//		long lastID = 0;
//		long lastTS = 0;
//		float lastV = Float.MIN_VALUE;
//		float timeUnit = 86400;
//		long timeUnitMin = (long) (timeUnit*1000);
//		
//		for(String record : records) {
//			String[] columns = record.split(",");
//			long nowID = Long.parseLong(columns[0].trim());
//			//long nowTS = new SimpleDateFormat(ProfilingInputFormat.defaultTimestampFormat, LocaleParser.parse(ProfilingInputFormat.defaultTimestampLocale)).parse(columns[1]).getTime();
//			float nowV = Float.parseFloat(columns[2].trim());
//			if(nowID == lastID) {
//				//long timeSpan = nowTS - lastTS;
//				
//				if(timeSpan != timeUnitMin) {
//					Text[] completedRecords = completeRecord(lastV, nowV, 0.1f, 2, lastID, nowID, new Text(record), timeSpan, timeUnitMin, ",");
//					for(int i = 0; i < completedRecords.length; i++) {
//						
//					}
//				}
//			}
//			lastID = nowID;
//			//lastTS = nowTS;
//			lastV = nowV;
//		}
	}
	
	public static Text[] completeRecord(float lastValue, float value,
			float defaultValue, int valueColNum,
			long lastID, long nowID,
			Text record, long timeSpan, long timeUnit, String colSaperator) {

		int missedRecordNum = (int) (timeSpan / timeUnit) - 1;
		String[] columns = record.toString().split(colSaperator);
		String nowValue = Float.toString(defaultValue);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columns.length; i++) {
			if (i == valueColNum) {
				sb.append(nowValue);
			} else {
				sb.append(columns[i]);
			}
		}
		Text completedRecord = new Text(sb.toString());
		Text[] completedRecords = new Text[missedRecordNum];
		for (int i = 0; i < missedRecordNum; i++) {
			completedRecords[i] = completedRecord;
		}
		return completedRecords;
	}
}
