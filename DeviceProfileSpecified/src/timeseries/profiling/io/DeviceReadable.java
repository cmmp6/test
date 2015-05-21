package timeseries.profiling.io;

public interface DeviceReadable {
	
	public String getDeviceID();
	
	public int getSensorID();
	
	public DeviceWritable getDevice();
}
