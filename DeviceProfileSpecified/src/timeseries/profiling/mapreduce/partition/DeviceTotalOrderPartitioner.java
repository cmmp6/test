package timeseries.profiling.mapreduce.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import timeseries.profiling.io.DeviceReadable;
import timeseries.profiling.utils.ArrayUtil;

public class DeviceTotalOrderPartitioner<K extends DeviceReadable & WritableComparable<?>, V>
		extends TotalOrderPartitioner<K, V> {

	private String[] splitPoints;
	public static final String DEFAULT_PATH = "partition.lst";
	public static final String PARTITIONER_PATH = "totalorder.partitioner.file.path";

	public DeviceTotalOrderPartitioner() {
		
	}
	
	@SuppressWarnings("deprecation")
	private String[] readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException {
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
		ArrayList<String> parts = new ArrayList<String>();
		Text key = new Text();
		NullWritable value = NullWritable.get();
		try {
			while (reader.next(key, value)) {
				parts.add(key.toString());
				key = new Text();
			}
			reader.close();
			reader = null;
		} finally {
			if(reader != null)
				reader.close();
		}

		return ArrayUtil.toStringArray(parts);
	}
	
	public static String getPartitionFile(Configuration conf) {
		return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
	}

	public void setConf(Configuration conf) {
		try {
			String parts = getPartitionFile(conf);
			final Path partFile = new Path(parts);
			final FileSystem fs = (DEFAULT_PATH.equals(parts)) ? FileSystem
					.getLocal(conf) // assume in DistributedCache
					: partFile.getFileSystem(conf);

			Job job = Job.getInstance(conf);
			splitPoints = readPartitions(fs, partFile, conf);
			if (splitPoints.length != job.getNumReduceTasks() - 1) {
				throw new IOException("Wrong number of partitions in keyset");
			}
			for (int i = 0; i < splitPoints.length - 1; ++i) {
				if (splitPoints[i].compareTo(splitPoints[i + 1]) >= 0) {
					throw new IOException("Split points are out of order");
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Can't read partitions file", e);
		}
	}

	public int getPartition(K key, V value, int numPartitions) {
	      final int pos = Arrays.binarySearch(splitPoints, key.getDeviceID()) + 1;
	      return (pos < 0) ? -pos : pos;
	}
}
