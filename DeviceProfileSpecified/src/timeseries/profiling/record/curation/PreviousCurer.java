package timeseries.profiling.record.curation;

import org.apache.hadoop.conf.Configuration;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.mapreduce.ProfilingInputFormat;
import timeseries.profiling.utils.RecordUtil;

public class PreviousCurer implements Curer {

	private int attrValueColNum;
	private int timestampColNum;
	private long timeUnitMicroSec;
	private String colSaperator;
	private String defaultValue;
	
	public PreviousCurer() {

	}
	
	public PreviousCurer(Configuration conf) {
		
		attrValueColNum = conf.getInt(ProfilingInputFormat.VALUE_COLUMN_NUMBER, ProfilingInputFormat.defaultValueStampColNum);
		timestampColNum = conf.getInt(ProfilingInputFormat.TIMESTAMP_COLUMN_NUMBER, ProfilingInputFormat.defaultTimeStampColNum);
		timeUnitMicroSec = (long)(conf.getFloat(ProfilingInputFormat.TIMESTAMP_UNIT, ProfilingInputFormat.defaultTimestampUnit)*ProfilingInputFormat.MICROSEC_TO_SEC);
		colSaperator = conf.get(ProfilingInputFormat.COLUMN_SAPERATOR, ProfilingInputFormat.defaultColSap);
		defaultValue = Float.toString(conf.getFloat(ProfilingInputFormat.DATA_CURATION_VALUE, ProfilingInputFormat.defaultDataCurationValue));
	}

	@Override
	//cure the records from normal record to normal record
	//regard records that can not be parsed as missed record and generated missed records according to the timespan
	public String[] cureRecord(BaseRecordKeyWritable key, float value, String record, BaseRecordKeyWritable preKey, float lastValue) {

		int missedRecordNum = (int) ((key.getTimestamp() - preKey.getTimestamp()) / timeUnitMicroSec) - 1;
		String nowValue = Float.toString(lastValue);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(RecordUtil.rebuildRecord(record, colSaperator, new int[]{timestampColNum, attrValueColNum}, new String[]{Long.toString((preKey.getTimestamp() + timeUnitMicroSec * (i + 1))), nowValue}));
		}
		return curedValuesWithTS;
	}

	@Override
	//cure the records from unparsedRecord to normal record
	public String[] cureUnparsedRecordtoNorRecord(BaseRecordKeyWritable key, float value, BaseRecordKeyWritable firstUnparsedKey, String firstUnparsedRecord) {
		
		String nowValue = Float.toString(value);
		int missedRecordNum = (int) ((key.getTimestamp() - firstUnparsedKey.getTimestamp()) / timeUnitMicroSec);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(firstUnparsedKey.getTimestamp() + timeUnitMicroSec * i) + "," + nowValue);
		}
		return curedValuesWithTS;
	}

	@Override
	//cure the records from unparsed record to unparsed record, use default value as the value
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
		
		String nowValue = Float.toString(preValue);

		int missedRecordNum = (int) ((lastUnparsedKey.getTimestamp() - preKey.getTimestamp()) / timeUnitMicroSec);

		String[] curedValuesWithTS = new String[missedRecordNum];
		for(int i = 0; i < missedRecordNum; i++) {
			curedValuesWithTS[i] = new String(Long.toString(preKey.getTimestamp() + timeUnitMicroSec * (i + 1)) + "," + nowValue);
		}
		return curedValuesWithTS;
	}
}