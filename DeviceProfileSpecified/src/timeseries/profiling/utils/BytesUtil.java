package timeseries.profiling.utils;

import java.util.Arrays;

public class BytesUtil {

	public static String toString(byte[] org, int start, int length) {
		return new String(Arrays.copyOfRange(org, start, length));
	}
	
	public static int toInt(byte[] org, int start) {
		int value;    
		value = (int) ((org[start]&0xFF)
				| ((org[start+1]<<8) & 0xFF00)
				| ((org[start+2]<<16)& 0xFF0000)
				| ((org[start+3]<<24) & 0xFF000000));
		return value;  
	}
}
