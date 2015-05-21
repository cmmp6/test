package timeseries.profiling.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.mapreduce.partition.DeviceInputSampler;
import timeseries.profiling.mapreduce.partition.DeviceInputSampler.DeviceSampler;
import timeseries.profiling.utils.LineReader;

public class ProfilingInputFormat extends
		FileInputFormat<BaseRecordKeyWritable, Text> {

	public static final String INPUT_HDFS_DIR = "input.hdfs.dir";
	public static final String OUTPUT_HDFS_DIR = "output.hdfs.dir";

	public static final String SAMPLER_NAME = "sampler.name";
	public static final String MAX_SAMPLED_SPLIT_NUMBER = "max.sampled.split.number";
	public static final String SAMPLER_FREQUENCY = "sampler.frequency";
	
	public static final String SENSORID_PRIOR = "sensorid.prior";
	
	public static final String DATA_SCHEMA = "device_id,sensor_id,timestamp,value";
	public static final String DATA_FIRST_LINE_SCHEMA = "data.first.line.schema";
	public static final String DEVICEID_COLUMN_NUMBER = "deviceid.column.number";
	public static final String SENSORID_COLUMN_NUMBER = "portid.column.number";
	public static final String TIMESTAMP_COLUMN_NUMBER = "timestamp.column.number";
	public static final String VALUE_COLUMN_NUMBER = "value.column.number";
	public static final String COLUMN_SAPERATOR = "column.saperator";
	public static final String TIMESTAMP_UNIT = "timestamp.unit";
	public static final String DATA_CURATION = "data.curation";
	public static final String DATA_CURATION_METHOD = "data.curation.method";
	public static final String DATA_CURATION_VALUE = "data.curation.value";
	
	public static final String DATA_FILE_ENCODE_FORMAT = "data.file.encode.format";
	
	public static final String REDUCE_TASK_NUMBER = "reduce.task.number";

	public static final boolean defaultFirstLineSchema = true;
	public static final int defaultDeviceIDColNum = 0;
	public static final int defaultSensorIDColNum = 1;
	public static final int defaultTimeStampColNum = 2;
	public static final int defaultValueStampColNum = 3;
	public static final String defaultColSap = ",";
	public static final int defaultMaxSampledSplitNum = 10;
	public static final float defaultSamplerFreq = 0.1f;
	public static final float defaultTimestampUnit = 1;
	public static final float defaultDataCurationValue = 1;
	public static final String defaultDataCurationMethod = "LAST";
	public static final boolean defaultDataCuration = true;
	public static final boolean defaultSensorIDPrior = false;
	public static final int defaultReduceTaskNum = 1;
	public static final String defaultDataFileEncodeFormat = "utf-8";
	public static final int MICROSEC_TO_SEC = 1000;
	
	@Override
	public RecordReader<BaseRecordKeyWritable, Text> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		return new ProfilingRecordReader(taskAttemptContext);
	}

	public static void computeAndWritePartitionFile(final Job job)
			throws IOException {
		Configuration conf = job.getConfiguration();
		final ProfilingInputFormat inputFormat = new ProfilingInputFormat();
		final List<InputSplit> splits = inputFormat.getSplits(job);

		DeviceSampler<BaseRecordKeyWritable, Text> sampler = null;
		int sampleSplits = Math.min(conf.getInt(MAX_SAMPLED_SPLIT_NUMBER,
				defaultMaxSampledSplitNum), splits.size());
		String samplerName = conf.get(SAMPLER_NAME, "SplitSampler");
		if (samplerName.equalsIgnoreCase("SplitSampler")) {
			sampler = new DeviceInputSampler.DeviceSplitSampler<BaseRecordKeyWritable, Text>(
					splits.size(), sampleSplits);
		} else if (samplerName.equalsIgnoreCase("RandomSampler")) {
			double randomSamplerFreq = conf.getDouble(SAMPLER_FREQUENCY,
					defaultSamplerFreq);
			sampler = new DeviceInputSampler.DeviceRandomSampler<BaseRecordKeyWritable, Text>(
					randomSamplerFreq, splits.size(), sampleSplits);
		} else if (samplerName.equalsIgnoreCase("IntervalSampler")) {
			double randomSamplerFreq = conf.getDouble(SAMPLER_FREQUENCY,
					defaultSamplerFreq);
			sampler = new DeviceInputSampler.DeviceIntervalSampler<BaseRecordKeyWritable, Text>(
					randomSamplerFreq, sampleSplits);
		}

		try {
			DeviceInputSampler.writePartitionFile(job, sampler);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	static class ProfilingRecordReader extends
			RecordReader<BaseRecordKeyWritable, Text> {
		
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private FSDataInputStream fileIn;
		private Seekable filePosition;
		private int maxLineLength;
		private BaseRecordKeyWritable key;
		private Text value;
		
		private Configuration conf;

		public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";

		public ProfilingRecordReader() {

		}
		
		public ProfilingRecordReader(TaskAttemptContext taskAttemptContext) {
			conf = taskAttemptContext.getConfiguration();
		}

		@Override
		public void close() throws IOException {
			if (in != null)
				in.close();
		}

		@Override
		public BaseRecordKeyWritable getCurrentKey()
				throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
			}
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();

			final FileSystem fs = file.getFileSystem(job);
			fileIn = fs.open(file);

			fileIn.seek(start);
			in = new LineReader(fileIn, job);

			filePosition = fileIn;
			if (start != 0) {
				start += in.readLine(new Text());
			}
			this.pos = start;
		}

		private long getFilePosition() throws IOException {
			long retVal;
			if (null != filePosition) {
				retVal = filePosition.getPos();
			} else {
				retVal = pos;
			}
			return retVal;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (key == null) {
				key = new BaseRecordKeyWritable();
			}

			if (value == null) {
				value = new Text();
			}
			
			int newSize = 0;
			while (getFilePosition() <= end) {
				newSize = in.readLine(value);
				
				//if there is empty line, regard this is the end of data file
				if(value.getLength() == 0) {
					break;
				}
				
				try {
					if(pos == 0 && conf.getBoolean(DATA_FIRST_LINE_SCHEMA, defaultFirstLineSchema)) { //read data schema
						pos += newSize;
						continue;
					}
					else {
						key = new BaseRecordKeyWritable(conf, value.toString());
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				pos += newSize;
				
				if (newSize < maxLineLength) {
					break;
				}
			}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}
	}
}
