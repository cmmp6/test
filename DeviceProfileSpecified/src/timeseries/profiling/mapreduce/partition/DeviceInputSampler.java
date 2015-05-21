package timeseries.profiling.mapreduce.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import timeseries.profiling.io.DeviceReadable;
import timeseries.profiling.mapreduce.ProfilingInputFormat;
import timeseries.profiling.utils.ArrayUtil;

public class DeviceInputSampler<K extends DeviceReadable, V> extends
		InputSampler<K, V> {

	public DeviceInputSampler(Configuration conf) {
		super(conf);
	}

	public interface DeviceSampler<K, V> {
		String[] getDeviceSample(InputFormat<K, V> inf, Job job)
				throws IOException, InterruptedException;
	}

	public static class DeviceSplitSampler<K extends DeviceReadable, V>
			extends SplitSampler<K, V> implements DeviceSampler<K, V> {

		protected int numSamples;
		protected int maxSplitsSampled;

		public DeviceSplitSampler(int numSamples) {
			super(numSamples);
			this.numSamples = numSamples;
			this.maxSplitsSampled = Integer.MAX_VALUE;
		}

		public DeviceSplitSampler(int numSamples, int maxSplitsSampled) {
			super(numSamples, maxSplitsSampled);
			this.numSamples = numSamples;
			this.maxSplitsSampled = maxSplitsSampled;
		}

		public String[] getDeviceSample(InputFormat<K, V> inf, Job job)
				throws IOException, InterruptedException {
			List<InputSplit> splits = inf.getSplits(job);
			ArrayList<String> samples = new ArrayList<String>(numSamples);
			int splitsToSample = Math.min(maxSplitsSampled, splits.size());
			int samplesPerSplit = numSamples / splitsToSample;
			long records = 0;
			for (int i = 0; i < splitsToSample; ++i) {
				TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
						job.getConfiguration(), new TaskAttemptID());
				RecordReader<K, V> reader = inf.createRecordReader(
						splits.get(i), samplingContext);
				reader.initialize(splits.get(i), samplingContext);
				while (reader.nextKeyValue()) {
					if(job.getConfiguration().getBoolean(ProfilingInputFormat.DATA_FIRST_LINE_SCHEMA, ProfilingInputFormat.defaultFirstLineSchema)) {  //ignore the first line as schema
						continue;
					}
					samples.add(reader.getCurrentKey().getDeviceID());
					++records;
					if ((i + 1) * samplesPerSplit <= records) {
						break;
					}
				}
				reader.close();
			}

			return ArrayUtil.toStringArray(samples);
		}
	}

	public static class DeviceRandomSampler<K extends DeviceReadable, V>
			extends RandomSampler<K, V> implements DeviceSampler<K, V> {

		protected double freq;
		protected int numSamples;
		protected int maxSplitsSampled;

		public DeviceRandomSampler(double freq, int numSamples) {
			super(freq, numSamples);
			this.freq = freq;
			this.numSamples = numSamples;
			this.maxSplitsSampled = Integer.MAX_VALUE;
		}

		public DeviceRandomSampler(double freq, int numSamples,
				int maxSplitsSampled) {
			super(freq, numSamples, maxSplitsSampled);
			this.freq = freq;
			this.numSamples = numSamples;
			this.maxSplitsSampled = maxSplitsSampled;
		}

		public String[] getDeviceSample(InputFormat<K, V> inf, Job job)
				throws IOException, InterruptedException {

			List<InputSplit> splits = inf.getSplits(job);
			ArrayList<String> samples = new ArrayList<String>(numSamples);
			int splitsToSample = Math.min(maxSplitsSampled, splits.size());

			Random r = new Random();
			long seed = r.nextLong();
			r.setSeed(seed);
			for (int i = 0; i < splits.size(); ++i) {
				InputSplit tmp = splits.get(i);
				int j = r.nextInt(splits.size());
				splits.set(i, splits.get(j));
				splits.set(j, tmp);
			}

			for (int i = 0; i < splitsToSample
					|| (i < splits.size() && samples.size() < numSamples); ++i) {
				TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
						job.getConfiguration(), new TaskAttemptID());
				RecordReader<K, V> reader = inf.createRecordReader(
						splits.get(i), samplingContext);
				reader.initialize(splits.get(i), samplingContext);
				while (reader.nextKeyValue()) {
					if(job.getConfiguration().getBoolean(ProfilingInputFormat.DATA_FIRST_LINE_SCHEMA, ProfilingInputFormat.defaultFirstLineSchema)) {  //ignore the first line as schema
						continue;
					}
					if (r.nextDouble() <= freq) {
						if (samples.size() < numSamples) {
							samples.add(reader.getCurrentKey().getDeviceID());
						} else {
							int ind = r.nextInt(numSamples);
							if (ind != numSamples) {
								samples.set(ind, reader.getCurrentKey().getDeviceID());
							}
							freq *= (numSamples - 1) / (double) numSamples;
						}
					}
				}
				reader.close();
			}

			return ArrayUtil.toStringArray(samples);
		}
	}

	public static class DeviceIntervalSampler<K extends DeviceReadable, V>
			extends IntervalSampler<K, V> implements DeviceSampler<K, V> {

		protected final double freq;
		protected int maxSplitsSampled;

		public DeviceIntervalSampler(double freq) {
			super(freq);
			this.freq = freq;
			this.maxSplitsSampled = Integer.MAX_VALUE;
		}

		public DeviceIntervalSampler(double freq, int maxSplitsSampled) {
			super(freq, maxSplitsSampled);
			this.freq = freq;
			this.maxSplitsSampled = maxSplitsSampled;
		}

		public String[] getDeviceSample(InputFormat<K, V> inf, Job job)
				throws IOException, InterruptedException {
			List<InputSplit> splits = inf.getSplits(job);
			ArrayList<String> samples = new ArrayList<String>();
			int splitsToSample = Math.min(maxSplitsSampled, splits.size());
			long records = 0;
			long kept = 0;
			for (int i = 0; i < splitsToSample; ++i) {
				TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
						job.getConfiguration(), new TaskAttemptID());
				RecordReader<K, V> reader = inf.createRecordReader(
						splits.get(i), samplingContext);
				reader.initialize(splits.get(i), samplingContext);
				while (reader.nextKeyValue()) {
					if(job.getConfiguration().getBoolean(ProfilingInputFormat.DATA_FIRST_LINE_SCHEMA, ProfilingInputFormat.defaultFirstLineSchema)) {  //ignore the first line as schema
						continue;
					}
					++records;
					if ((double) kept / records < freq) {
						samples.add(reader.getCurrentKey().getDeviceID());
						++kept;
					}
				}
				reader.close();
			}
			return ArrayUtil.toStringArray(samples);
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation", "rawtypes" })
	public static <K, V> void writePartitionFile(Job job,
			DeviceSampler<K, V> deviceSampler) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = job.getConfiguration();
		final InputFormat inf = ReflectionUtils.newInstance(
				job.getInputFormatClass(), conf);
		int numPartitions = job.getNumReduceTasks();
		String[] samples = deviceSampler.getDeviceSample(inf, job);
		Arrays.sort(samples);
		Path dst = new Path(DeviceTotalOrderPartitioner.getPartitionFile(conf));
		FileSystem fs = dst.getFileSystem(conf);
		if (fs.exists(dst)) {
			fs.delete(dst, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst,
				Text.class, NullWritable.class);
		NullWritable nullValue = NullWritable.get();
		float stepSize = samples.length / (float) numPartitions;
		int last = -1;
		for (int i = 1; i < numPartitions; ++i) {
			int k = Math.round(stepSize * i);
			while (last >= k && samples[last] == samples[k]) {
				++k;
			}
			writer.append(new Text(samples[k]), nullValue);
			last = k;
		}
		writer.close();
	}
}
