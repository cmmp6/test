package timeseries.profiling.record.curation;

import org.apache.hadoop.conf.Configuration;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.mapreduce.ProfilingInputFormat;

public class LinageCurer implements Curer {
	
	private long timeUnitMicroSec;
	
	private PreviousCurer previousCurer;

	public LinageCurer() {
		previousCurer = new PreviousCurer();
	}
	
	public LinageCurer(Configuration conf) {
		
		previousCurer = new PreviousCurer(conf);
		
		timeUnitMicroSec = (long)(conf.getFloat(ProfilingInputFormat.TIMESTAMP_UNIT, ProfilingInputFormat.defaultTimestampUnit)*ProfilingInputFormat.MICROSEC_TO_SEC);
	}

	@Override
	//regard records that can not be parsed as missed record and generated missed records according to the timespan
	public String[] cureRecord(BaseRecordKeyWritable key, float value, String record, BaseRecordKeyWritable preKey, float preValue) {
		
		int missedRecordNum = (int) ((key.getTimestamp() - preKey.getTimestamp()) / timeUnitMicroSec) - 1;
		float valueUnit = (value - preValue) / (missedRecordNum + 1);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for (int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString((preKey.getTimestamp() + timeUnitMicroSec * (i + 1))) + "," + Float.toString(preValue + valueUnit * (i + 1)));
		}
		return curedValuesWithTS;
	}

	@Override
	public String[] cureUnparsedRecordtoNorRecord(BaseRecordKeyWritable key, float value, BaseRecordKeyWritable firstUnparsedKey,
			String firstUnparsedRecord) {
		return previousCurer.cureUnparsedRecordtoNorRecord(key, value, firstUnparsedKey, firstUnparsedRecord);
	}

	@Override
	public String[] cureRecord(BaseRecordKeyWritable firstUnparsedKey, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord) {
		return previousCurer.cureRecord(firstUnparsedKey, lastUnparsedKey, lastUnparsedRecord);
	}
	
	@Override
	public String[] cureNorRecordtoUnparsedRecord(BaseRecordKeyWritable preKey, float preValue, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord) {
		return previousCurer.cureNorRecordtoUnparsedRecord(preKey, preValue, lastUnparsedKey, lastUnparsedRecord);
	}
}
