package timeseries.profiling.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.record.curation.Curer;
import timeseries.profiling.record.curation.DefaultCurer;
import timeseries.profiling.record.curation.LinageCurer;
import timeseries.profiling.record.curation.PreviousCurer;

public class ProfilingReducer extends Reducer<BaseRecordKeyWritable, Text, NullWritable, Text> {
	
	
	private BaseRecordKeyWritable preKey;
	private float preAttrValue;
	private BaseRecordKeyWritable firstUnparsedKey;
	private String firstUnparsedRecord;
	private BaseRecordKeyWritable lastUnparsedKey;
	private String lastUnparsedRecord;
	
	private int attrValueColNum;
	private long timeUnitMicroSec;
	private String colSaperator;
	private boolean dataCurationSign;
	private String dataCurationMethod;
	
	private Curer curer;
	
	public void setup(Context context) throws IOException, InterruptedException {
		
		preKey = new BaseRecordKeyWritable();
		preAttrValue = Float.MIN_VALUE;
		firstUnparsedKey = new BaseRecordKeyWritable();
		lastUnparsedKey = new BaseRecordKeyWritable();
		
		Configuration conf = context.getConfiguration();
		attrValueColNum = conf.getInt(ProfilingInputFormat.VALUE_COLUMN_NUMBER, ProfilingInputFormat.defaultValueStampColNum);
		timeUnitMicroSec = (long)(conf.getFloat(ProfilingInputFormat.TIMESTAMP_UNIT, ProfilingInputFormat.defaultTimestampUnit)*ProfilingInputFormat.MICROSEC_TO_SEC);
		colSaperator = conf.get(ProfilingInputFormat.COLUMN_SAPERATOR, ProfilingInputFormat.defaultColSap);
		dataCurationSign = conf.getBoolean(ProfilingInputFormat.DATA_CURATION, ProfilingInputFormat.defaultDataCuration);
		
		if(dataCurationSign == true) {
			dataCurationMethod = conf.get(ProfilingInputFormat.DATA_CURATION_METHOD, ProfilingInputFormat.defaultDataCurationMethod);
			
			if(dataCurationMethod.equalsIgnoreCase("DEFAULT")) {
				curer = new DefaultCurer(conf);
			}
			else if(dataCurationMethod.equalsIgnoreCase("LINAGE")) {
				curer = new LinageCurer(conf);
			}
			else {
				curer = new PreviousCurer(conf);
			}
		}
		
		context.write(NullWritable.get(), new Text(ProfilingInputFormat.DATA_SCHEMA));
	}
	
	public void curationReduce(BaseRecordKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//for every Device&TS, only get the first value
		for(Text t : values) {
			
			String record = t.toString();
			float attrValue = Float.MIN_VALUE;
			try{
				attrValue = Float.parseFloat(record.split(colSaperator)[attrValueColNum].trim());
			} catch(NumberFormatException numberFormatEx) {
				System.out.println("Fail to parse value");
				
				if (key.getDevice().equals(preKey.getDevice()) == false && key.getDevice().equals(firstUnparsedKey.getDevice()) == false) {
					if(lastUnparsedKey.getTimestamp() != Long.MIN_VALUE) {
						cureToUnparsedRecord(context);
						lastUnparsedKey = new BaseRecordKeyWritable();
					}
					firstUnparsedKey = new BaseRecordKeyWritable(key.getDevice(), key.getTimestamp());
					firstUnparsedRecord = record;
				}
				else {
					lastUnparsedKey = new BaseRecordKeyWritable(key.getDevice(), key.getTimestamp());
					lastUnparsedRecord = record;
				}
				break;
			}

			if(key.getDevice().equals(preKey.getDevice()) || key.getDevice().equals(firstUnparsedKey.getDevice())) {
				cureToParsedRecord(context, key, attrValue, record);
			}
			else if (lastUnparsedKey.getTimestamp() != Long.MIN_VALUE) {
				cureToUnparsedRecord(context);
			}

			preKey = new BaseRecordKeyWritable(key.getDevice(), key.getTimestamp());
			preAttrValue = attrValue;
			firstUnparsedKey = new BaseRecordKeyWritable(); //reset first and last after curing
			lastUnparsedKey = new BaseRecordKeyWritable();
			context.write(NullWritable.get(), new Text(key.toString() + "," + attrValue));
			break;
		}
	}
	
	public void cureToUnparsedRecord(Context context) throws IOException, InterruptedException {
		
		String[] curedValuesWithTS = new String[0];
		if(firstUnparsedKey.getTimestamp() == Long.MIN_VALUE) { //normal to unnormal
			curedValuesWithTS = curer.cureNorRecordtoUnparsedRecord(preKey, preAttrValue, lastUnparsedKey, lastUnparsedRecord);
		}
		else { //unnormal to unnormal
			curedValuesWithTS = curer.cureRecord(firstUnparsedKey, lastUnparsedKey, lastUnparsedRecord);
		}
		
		if(curedValuesWithTS.length > 0) {
			for (String curedValue : curedValuesWithTS) {
				context.write(NullWritable.get(), new Text(lastUnparsedKey.getDevice().toString() + "," + curedValue));
			}
		}
	}
	
	public void cureToParsedRecord(Context context, BaseRecordKeyWritable key, float attrValue, String record) throws IOException, InterruptedException {
		
		String[] curedValuesWithTS = new String[0];
		if(firstUnparsedKey.getTimestamp() != Long.MIN_VALUE) {//unnormal to normal
			curedValuesWithTS = curer.cureUnparsedRecordtoNorRecord(key, attrValue, firstUnparsedKey, firstUnparsedRecord);
		}
		else if((key.getTimestamp() - preKey.getTimestamp()) > timeUnitMicroSec) {//normal to normal
			curedValuesWithTS = curer.cureRecord(key, attrValue, record, preKey, preAttrValue);
		}
		
		if(curedValuesWithTS.length > 0) {
			for (String curedValue : curedValuesWithTS) {
				context.write(NullWritable.get(), new Text(key.getDevice().toString() + "," + curedValue));
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {

		if(lastUnparsedKey.getTimestamp() != Long.MIN_VALUE) {
			cureToUnparsedRecord(context);
		}
	}
	
	public void nonCurationReduce(BaseRecordKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text t : values) {

			long timeSpan = key.getTimestamp() - preKey.getTimestamp();
			if (timeSpan <= 0 && preKey.getTimestamp() != Long.MIN_VALUE) {
				throw new InterruptedException("Timestamp <= last timestamp!");
			}

			float attrValue = 0;
			try {
				attrValue = Float.parseFloat(t.toString().split(colSaperator)[attrValueColNum].trim());
			} catch (NumberFormatException numberFormatEx) {
				context.write(NullWritable.get(), new Text(key.toString() + ","));
				System.out.println("Fail to parse value");	
				break;
			}
			
			context.write(NullWritable.get(), new Text(key.toString() + "," + attrValue));
			break;
		}
	}
	
	
	public void runCurationReduce(Context context) throws IOException, InterruptedException {
		while (context.nextKey()) {
			curationReduce(context.getCurrentKey(), context.getValues(), context);
			// If a back up store is used, reset it
			Iterator<Text> iter = context.getValues().iterator();
			if (iter instanceof ReduceContext.ValueIterator) {
				((ReduceContext.ValueIterator<Text>) iter)
						.resetBackupStore();
			}
		}
	}
	
	public void runNonCurationReduce(Context context) throws IOException, InterruptedException {
		while (context.nextKey()) {
			nonCurationReduce(context.getCurrentKey(), context.getValues(), context);
			// If a back up store is used, reset it
			Iterator<Text> iter = context.getValues().iterator();
			if (iter instanceof ReduceContext.ValueIterator) {
				((ReduceContext.ValueIterator<Text>) iter)
						.resetBackupStore();
			}
		}
	}
	
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			if(dataCurationSign == true) {
				runCurationReduce(context);
			}
			else {
				runNonCurationReduce(context);
			}
		} finally {
			cleanup(context);
		}
	}

}