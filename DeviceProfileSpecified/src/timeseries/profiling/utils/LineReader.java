package timeseries.profiling.utils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import timeseries.profiling.mapreduce.ProfilingInputFormat;

public class LineReader implements Closeable {

	private BufferedReader br;
	//private byte[] recordDelimiterBytes;
	
	public LineReader() {
		
	}
	
	public LineReader(InputStream in, Configuration conf) throws UnsupportedEncodingException {
		br = new BufferedReader(new InputStreamReader(in, conf.get(ProfilingInputFormat.DATA_FILE_ENCODE_FORMAT, ProfilingInputFormat.defaultDataFileEncodeFormat)));
	}
	
	public LineReader(InputStream in, byte[] recordDelimiterBytes) {
		this.br = new BufferedReader(new InputStreamReader(in));
	}
	
	public int readLine(Text str) throws IOException {
		str.clear();
		String line = br.readLine();
		if(line != null) {
			str.set(line);
			return str.getLength();
		}
		else
			return 0;
	}

	@Override
	public void close() throws IOException {
		br.close();
	}
	
	
}
