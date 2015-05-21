package timeseries.profiling.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class Preprocessor {

	public static void main(String[] args) throws IOException {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:\\dataProfiling\\data\\废气排口污染物日数据.csv")), "gbk"));
		HashMap<String, ArrayList<String>> attr2Records = new HashMap<String, ArrayList<String>>();
		int attrColNum = 5;
		
		String line = null;
		String firstLine = br.readLine();
		while((line = br.readLine()) != null) {
			String[] columns = line.split(",");
			String attrName = columns[attrColNum];
			if(attr2Records.containsKey(attrName) == false) {
				ArrayList<String> records = new ArrayList<String>();
				attr2Records.put(attrName, records);
			}
			attr2Records.get(attrName).add(line);
		}
		br.close();
		
		for(String attrName : attr2Records.keySet()) {
			ArrayList<String> records = attr2Records.get(attrName);
			int fileSeq = 0;
			BufferedWriter bw = null;
			for(int i = 0; i < records.size(); i++) {
				if(i%1000 == 0) {
					if(bw != null)
						bw.close();
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\dataProfiling\\data\\废气排口污染物日数据-" + attrName + "-" + fileSeq + ".csv"))));
					bw.write(firstLine + "\n");
					fileSeq++;
				}
				bw.write(records.get(records.size() - i - 1) + "\n");
			}
			bw.close();
		}
	}
}
