package timeseries.profiling.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import timeseries.profiling.io.BaseRecordKeyWritable;
import timeseries.profiling.mapreduce.partition.DeviceTotalOrderPartitioner;

public class ProfilingDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		//conf.set("fs.default.name","hdfs://192.168.20.128:9000"); //for test remotely
		//conf.set("mapred.job.tracker","http://192.168.20.128:9001"); //for test remotely

		Job job = Job.getInstance(conf);

		job.setJobName("TimeSeries Data Profiling");
	    job.setJarByClass(ProfilingDriver.class);
	    //job.setJar("C:\\Users\\Ming\\Desktop\\dataprofiling.jar"); //for test remotely

	    job.setInputFormatClass(ProfilingInputFormat.class);
	    job.setPartitionerClass(DeviceTotalOrderPartitioner.class);
	    if(conf.getBoolean(ProfilingInputFormat.SENSORID_PRIOR, ProfilingInputFormat.defaultSensorIDPrior) == true)
	    	job.setSortComparatorClass(BaseRecordKeyWritable.SensorIDPriorComparator.class);
	    else
	    	job.setSortComparatorClass(BaseRecordKeyWritable.Comparator.class);
	    
	    job.setMapperClass(ProfilingMapper.class);
	    job.setReducerClass(ProfilingReducer.class);
	    
	    job.setMapOutputKeyClass(BaseRecordKeyWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    ProfilingInputFormat.setInputPaths(job, new Path(conf.get(ProfilingInputFormat.INPUT_HDFS_DIR)));
	    Path outputPath = new Path(conf.get(ProfilingInputFormat.OUTPUT_HDFS_DIR));
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
	    TextOutputFormat.setOutputPath(job, outputPath);
	    
	    job.setNumReduceTasks(conf.getInt(ProfilingInputFormat.REDUCE_TASK_NUMBER, ProfilingInputFormat.defaultReduceTaskNum));
	    
	    ProfilingInputFormat.computeAndWritePartitionFile(job);
	    job.addCacheFile(new URI(new Path(DeviceTotalOrderPartitioner.getPartitionFile(conf)).toString()));
	    
	    int ret = job.waitForCompletion(true) ? 0 : 1;
		return ret;
	}

	/*
	 * args[0] the properties file of timeseries profiling Job configuration
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Properties properties = new Properties();
		properties.load(new FileInputStream(new File(args[0])));
		for(Object keyObject : properties.keySet()){
			String keyString = (String)keyObject;
			String value = properties.getProperty(keyString);
			
			if(keyString.equalsIgnoreCase(ProfilingInputFormat.DATA_FIRST_LINE_SCHEMA)) {
				conf.setBoolean(keyString, Boolean.parseBoolean(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.DEVICEID_COLUMN_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.SENSORID_COLUMN_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.TIMESTAMP_COLUMN_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.VALUE_COLUMN_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.MAX_SAMPLED_SPLIT_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.SAMPLER_FREQUENCY)) {
				conf.setDouble(keyString, Double.parseDouble(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.TIMESTAMP_UNIT)) {
				conf.setFloat(keyString, Float.parseFloat(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.DATA_CURATION)) {
				conf.setBoolean(keyString, Boolean.parseBoolean(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.SENSORID_PRIOR)) {
				conf.setBoolean(keyString, Boolean.parseBoolean(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.DATA_CURATION_METHOD)) {
				conf.set(keyString, value);
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.DATA_CURATION_VALUE)) {
				conf.setFloat(keyString, Float.parseFloat(value));
			}
			else if(keyString.equalsIgnoreCase(ProfilingInputFormat.REDUCE_TASK_NUMBER)) {
				conf.setInt(keyString, Integer.parseInt(value));
			}
			else {
				conf.set(keyString, value);
			}
		}
		
		int res = ToolRunner.run(conf, new ProfilingDriver(), args);
		System.exit(res);
	}
}
