package com.sky.mahout;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;

public class PrintStreamConverter implements OutputCollector<String, List<Pair<List<String>, Long>>> {
	private final PrintStream collector;

	private Map<String, String> icd9Map = new HashMap<String, String>();

	public PrintStreamConverter(PrintStream collector) {
		this.collector = collector;
		List<String> icd9List = null;
		try {
			icd9List = FileUtils.readLines(new File(AppConfig.ICD9_FILE_PATH));
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (String line : icd9List) {
			String[] item = line.split(",");
			String icd9 = item[0].startsWith("0") ? item[0].substring(1) : item[0];
			icd9Map.put(icd9, item[1]);
		}
	}

	@Override
	public void collect(String key, List<Pair<List<String>, Long>> values) throws IOException {
		for (Pair<List<String>, Long> pair : values) {
			String[] itemSet = pair.getFirst().get(0).split(",");
			StringBuilder sb = new StringBuilder();
			for (String item : itemSet) {
				sb.append("," + icd9Map.get(item));
			}
			collector.printf("%-20s %.20s(%s)\n", key, sb.substring(1), pair.getSecond()); 

		}
	}
}