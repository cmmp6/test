package timeseries.profiling.record.curation;

import timeseries.profiling.io.BaseRecordKeyWritable;

public interface Curer {

	//normal record to normal record
	String[] cureRecord(BaseRecordKeyWritable key, float value, String record, BaseRecordKeyWritable preKey, float preValue);
	
	//unparsed record to normal
	String[] cureUnparsedRecordtoNorRecord(BaseRecordKeyWritable key, float value, BaseRecordKeyWritable firstUnparsedKey, String firstUnparsedRecord);
	
	//normal to unparsed
	String[] cureNorRecordtoUnparsedRecord(BaseRecordKeyWritable preKey, float preValue, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord);
	
	//unparsed record to unparsed record
	String[] cureRecord(BaseRecordKeyWritable firstUnparsedKey, BaseRecordKeyWritable lastUnparsedKey, String lastUnparsedRecord);
}