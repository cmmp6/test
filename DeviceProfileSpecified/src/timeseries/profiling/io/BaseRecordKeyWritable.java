package timeseries.profiling.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import timeseries.profiling.mapreduce.ProfilingInputFormat;

public class BaseRecordKeyWritable implements DeviceReadable, WritableComparable<BaseRecordKeyWritable> {

	public static final int LONG_BYTE_LENGTH = 8;
	
	private DeviceWritable device;
	private long timestamp;
	
	public BaseRecordKeyWritable() {
		this.device = new DeviceWritable();
		this.timestamp = Long.MIN_VALUE;
	}
	
	public BaseRecordKeyWritable(String deviceID, int sensorID, long timestamp) {
		set(deviceID, sensorID, timestamp);
	}
	
	public BaseRecordKeyWritable(DeviceWritable device, long timestamp) {
		set(device, timestamp);
	}
	
	public BaseRecordKeyWritable(Configuration conf, String record) throws NumberFormatException, ParseException {
		
		int deviceIDColNum = conf.getInt(ProfilingInputFormat.DEVICEID_COLUMN_NUMBER, ProfilingInputFormat.defaultDeviceIDColNum);
		int sensorIDColNum = conf.getInt(ProfilingInputFormat.SENSORID_COLUMN_NUMBER, ProfilingInputFormat.defaultSensorIDColNum);
		int tsColNum = conf.getInt(ProfilingInputFormat.TIMESTAMP_COLUMN_NUMBER, ProfilingInputFormat.defaultTimeStampColNum);
		String colSaperator = conf.get(ProfilingInputFormat.COLUMN_SAPERATOR, ProfilingInputFormat.defaultColSap);
		String[] columns = record.split(colSaperator);
		set(columns[deviceIDColNum].trim(), Integer.parseInt(columns[sensorIDColNum]), Long.parseLong(columns[tsColNum]));
	}
	
	public void set(String deviceID, int sensorID, long timestamp) {
		set(new Text(deviceID), sensorID, timestamp);
	}
	
	public void set(Text deviceID, int sensorID, long timestamp) {
		this.device = new DeviceWritable(deviceID, sensorID);
		this.timestamp = timestamp;
	}
	
	public void set(DeviceWritable device, long timestamp) {
		set(device.getDeviceID(), device.getSensorID(), timestamp);
	}
	
	public DeviceWritable getDevice() {
		return this.device;
	}
	
	public String getDeviceID() {
		return this.device.getDeviceID();
	}
	
	public int getSensorID() {
		return this.device.getSensorID();
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}

	public void setDeviceID(String deviceID) {
		this.device.setDeviceID(deviceID);
	}
	
	public void setSensorID(int sensorID) {
		this.device.setSensorID(sensorID);
	}
	
	public void setDevice(DeviceWritable device) {
		this.device = device;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.device.readFields(dataInput);
		this.timestamp = dataInput.readLong();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.device.write(dataOutput);
		dataOutput.writeLong(timestamp);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.device.toString());
		sb.append(",");
		sb.append(Long.toString(timestamp));
		return sb.toString();
	}

	@Override
	public int compareTo(BaseRecordKeyWritable seriesRecordKey) {
		if(this.device.equals(seriesRecordKey.getDevice()) == false) {
			return this.device.compareTo(seriesRecordKey.getDevice());
		}
		else {
			return (this.timestamp < seriesRecordKey.getTimestamp() ? -1 : (this.timestamp == seriesRecordKey.getTimestamp() ? 0 : 1));
		}
	}
	
	public static class Comparator extends WritableComparator {
	
		public Comparator() {
			super(BaseRecordKeyWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int deviceComRes = DeviceWritable.Comparator.compareBytes(b1, s1, l1-LONG_BYTE_LENGTH, b2, s2, l2-LONG_BYTE_LENGTH);
			return deviceComRes != 0 ? deviceComRes : LongWritable.Comparator.compareBytes(b1, s1+l1-LONG_BYTE_LENGTH, LONG_BYTE_LENGTH, b2, s2+l2-LONG_BYTE_LENGTH, LONG_BYTE_LENGTH);
		}
	}
	
	
	public static class SensorIDPriorComparator extends WritableComparator {
		
		public SensorIDPriorComparator() {
			super(BaseRecordKeyWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int deviceComRes = DeviceWritable.SensorIDPriorComparator.compareBytes(b1, s1, l1-LONG_BYTE_LENGTH, b2, s2, l2-LONG_BYTE_LENGTH);
			return deviceComRes != 0 ? deviceComRes : LongWritable.Comparator.compareBytes(b1, s1+l1-LONG_BYTE_LENGTH, LONG_BYTE_LENGTH, b2, s2+l2-LONG_BYTE_LENGTH, LONG_BYTE_LENGTH);
		}
	}
	
	static { // register default comparator
		WritableComparator.define(BaseRecordKeyWritable.class, new Comparator());
	}
}
