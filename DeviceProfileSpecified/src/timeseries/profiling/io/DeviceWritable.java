package timeseries.profiling.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class DeviceWritable implements DeviceReadable, WritableComparable<DeviceWritable> {

	public static final String NULL_DEVICE_ID = "";
	public static final int INT_BYTE_LENGTH = 4;
	
	protected Text deviceID;
	protected int sensorID;
	
	public DeviceWritable() {
		deviceID = new Text(NULL_DEVICE_ID);
		sensorID = Integer.MIN_VALUE;
	}
	
	public DeviceWritable(String deviceID, int sensorID) {
		setDevice(new Text(deviceID), sensorID);
	}
	
	public DeviceWritable(Text deviceID, int sensorID) {
		setDevice(deviceID, sensorID);
	}
	
	public void setDevice(Text deviceID, int sensorID) {
		setDeviceID(deviceID);
		setSensorID(sensorID);
	}
	
	public void setDeviceID(String deviceID) {
		setDeviceID(new Text(deviceID));
	}
	
	public void setDeviceID(Text deviceID) {
		this.deviceID = deviceID;
	}
	
	public void setSensorID(int sensorID) {
		this.sensorID = sensorID;
	}
	
	public String getDeviceID() {
		return this.deviceID.toString();
	}
	
	public Text getTextDeviceID() {
		return this.deviceID;
	}
	
	public int getSensorID() {
		return this.sensorID;
	}
	
	public String toString() {
		return deviceID.toString() + "," + Integer.toString(sensorID);
	}
	
	public boolean equals(DeviceWritable otherDevice) {
		if(this.compareTo(otherDevice) == 0) {
			return true;
		}
		else
			return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		deviceID.readFields(in);
		this.sensorID = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		deviceID.write(out);
		out.writeInt(sensorID);
	}

	@Override
	public int compareTo(DeviceWritable otherDevice) {
		if(this.deviceID.equals(otherDevice) == false) {
			return this.deviceID.compareTo(otherDevice.getTextDeviceID());
		}
		else {
			return (this.sensorID < otherDevice.getSensorID() ? -1 : (this.sensorID == otherDevice.getSensorID() ? 0 : 1));
		}
	}
	
	public static class Comparator extends WritableComparator {
		
		public Comparator() {
			super(DeviceWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
		
		public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			int deviceIDComRes = Text.Comparator.compareBytes(b1, s1+n1, l1-n1-INT_BYTE_LENGTH, b2, s2+n2, l2-n2-INT_BYTE_LENGTH);
			return deviceIDComRes != 0 ? deviceIDComRes : IntWritable.Comparator.compareBytes(b1, s1+l1-INT_BYTE_LENGTH, INT_BYTE_LENGTH, b2, s2+l2-INT_BYTE_LENGTH, INT_BYTE_LENGTH);
		}
	}
	
	public static class SensorIDPriorComparator extends WritableComparator {
		
		public SensorIDPriorComparator() {
			super(DeviceWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
		
		public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			int sensorIDComRes = IntWritable.Comparator.compareBytes(b1, s1+l1-INT_BYTE_LENGTH, INT_BYTE_LENGTH, b2, s2+l2-INT_BYTE_LENGTH, INT_BYTE_LENGTH);
			return sensorIDComRes != 0 ? sensorIDComRes : Text.Comparator.compareBytes(b1, s1+n1, l1-n1-INT_BYTE_LENGTH, b2, s2+n2, l2-n2-INT_BYTE_LENGTH);
		}
	}
	
	static { // register default comparator
		WritableComparator.define(DeviceWritable.class, new Comparator());
	}

	@Override
	public DeviceWritable getDevice() {
		return this;
	}
}
