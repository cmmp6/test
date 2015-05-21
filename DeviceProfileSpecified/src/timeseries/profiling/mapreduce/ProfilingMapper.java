package timeseries.profiling.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import timeseries.profiling.io.BaseRecordKeyWritable;

public class ProfilingMapper extends Mapper<BaseRecordKeyWritable, Text, BaseRecordKeyWritable, Text> {

	public void map(BaseRecordKeyWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
