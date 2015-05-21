package timeseries.profiling.utils;

import java.util.List;

public class ArrayUtil {

	public static long[] toLongArray(List<Long> org) {
		long[] res = new long[org.size()];
		for(int i = 0; i < org.size(); i++) {
			res[i] = org.get(i);
		}
		return res;
	}
	
	public static String[] toStringArray(List<String> org) {
		String[] res = new String[org.size()];
		for(int i = 0; i < org.size(); i++) {
			res[i] = org.get(i);
		}
		return res;
	}
	
	public static String toString(String[] org, String separator) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < (org.length - 1); i++) {
			sb.append(org[i]);
			sb.append(separator);
		}
		sb.append(org[org.length - 1]);
		return sb.toString();
	}
}
