package timeseries.profiling.utils;

public class RecordUtil {

	/*
	 * record		original record
	 * Separator	record separator
	 * positions	positions need to be replaced
	 * cells		values used to replace old value in record
	 */
	public static String rebuildRecord(String record, String separator, int[] positions, String[] cells) {
		
		String[] columns = record.split(separator);
		for(int i = 0; i < positions.length; i++) {
			columns[positions[i]] = cells[i];
		}
		
		return ArrayUtil.toString(columns, separator);
	}
	
	/*
	 * record		original record
	 * Separator	record separator
	 * positions	positions need to be replaced
	 * cells		values used to replace old value in record
	 */
	public static String rebuildRecord(String record, String separator, int[] positions, float[] cells) {
		
		String[] columns = record.split(separator);
		for(int i = 0; i < positions.length; i++) {
			columns[positions[i]] = Float.toString(cells[i]);
		}
		
		return ArrayUtil.toString(columns, separator);
	}
}
