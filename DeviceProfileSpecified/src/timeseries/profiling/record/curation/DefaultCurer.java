package timeseries.profiling.record.curation;

import org.apache.hadoop.conf.Configuration;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.mapreduce.ProfilingInputFormat;

public class DefaultCurer implements Curer {

	private long timeUnitMicroSec;
	private String defaultValue;
	
	public DefaultCurer() {

	}
	
	public DefaultCurer(Configuration conf) {
		
		timeUnitMicroSec = (long)(conf.getFloat(ProfilingInputFormat.TIMESTAMP_UNIT, ProfilingInputFormat.defaultTimestampUnit)*ProfilingInputFormat.MICROSEC_TO_SEC);
		defaultValue = Float.toString(conf.getFloat(ProfilingInputFormat.DATA_CURATION_VALUE, ProfilingInputFormat.defaultDataCurationValue));

	}

	@Override
	//regard records that can not be parsed as missed record and generated missed records according to the timespan
	public String[] cureRecord(BaseRecordKeyWritable key, float value, String record, BaseRecordKeyWritable preKey, float lastValue) {
		
		int missedRecordNum = (int) ((key.getTimestamp() - preKey.getTimestamp()) / timeUnitMicroSec) - 1;
		
		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(preKey.getTimestamp() + timeUnitMicroSec * (i + 1)) + "," + defaultValue);
		}
		return curedValuesWithTS;
	}

	@Override
	//cure the records from unparsedRecord to normal record
	public String[] cureUnparsedRecordtoNorRecord(BaseRecordKeyWritable key, float value, BaseRecordKeyWritable firstUnparsedKey, String firstUnparsedRecord) {
		
		int missedRecordNum = (int) ((key.getTimestamp() - firstUnparsedKey.getTimestamp()) / timeUnitMicroSec);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for (int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(firstUnparsedKey.getTimestamp() + timeUnitMicroSec * i) + "," + defaultValue);
		}
		return curedValuesWithTS;
		
	}

	@Override
	//cure the records from unparsed record to unparsed record, use preValue as the value
	public String[] cureRecord(BaseRecordKeyWritable firstUnparsedKey, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord) {
		
		int missedRecordNum = (int) ((lastUnparsedKey.getTimestamp() - firstUnparsedKey.getTimestamp()) / timeUnitMicroSec) + 1;

		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(firstUnparsedKey.getTimestamp() + timeUnitMicroSec * i) + "," + defaultValue);
		}
		return curedValuesWithTS;
	}

	@Override
	//cure the records from normal record to unparsed record, use preValue as the value
	public String[] cureNorRecordtoUnparsedRecord(BaseRecordKeyWritable preKey, float preValue, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord) {
		
		int missedRecordNum = (int) ((lastUnparsedKey.getTimestamp() - preKey.getTimestamp()) / timeUnitMicroSec);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(preKey.getTimestamp() + timeUnitMicroSec * (i + 1)) + "," + defaultValue);
		}
		return curedValuesWithTS;
	}
}