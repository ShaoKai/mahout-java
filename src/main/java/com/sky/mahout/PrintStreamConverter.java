package com.sky.mahout;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;

public class PrintStreamConverter implements OutputCollector<String, List<Pair<List<String>, Long>>> {
	private final PrintStream collector;
	private static Map<String, String> nodesMap = new HashMap<String, String>();
	private static Map<String, Set<String>> edgeMap = new HashMap<String, Set<String>>();
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

	static String gencode() {
		String[] letters = new String[15];
		letters = "0123456789ABCDEF".split("");
		String code = "#";
		for (int i = 0; i < 6; i++) {
			double ind = (Math.random() * 14) + 1;
			int index = (int) Math.round(ind);
			code += letters[index];
		}
		return code;
	}

	public static void print() {
		StringBuilder sb = new StringBuilder();
		for (Iterator<Entry<String, String>> itr = nodesMap.entrySet().iterator(); itr.hasNext();) {
			Entry<String, String> entry = itr.next();
			sb.append("," + entry.getKey() + ":{'color':'" + gencode() + "','label':'" + entry.getValue() + "'}\n");
		}
		System.out.println("nodes:{\n" + sb.substring(1) + "},");

		sb = new StringBuilder();
		for (Iterator<Entry<String, Set<String>>> itr = edgeMap.entrySet().iterator(); itr.hasNext();) {
			Entry<String, Set<String>> entry = itr.next();
			StringBuilder innerSb = new StringBuilder();
			for (String node : entry.getValue()) {
				innerSb.append(", " + node + ":{}");
			}
			sb.append("," + entry.getKey() + ":{ " + innerSb.substring(1) + " }\n");
		}

		System.out.println("edges:{\n" + sb.substring(1) + "}");
	}

	@Override
	public void collect(String key, List<Pair<List<String>, Long>> values) throws IOException {
		for (Pair<List<String>, Long> pair : values) {
			String[] itemSet = pair.getFirst().get(0).split(",");

			StringBuilder sb = new StringBuilder();
			for (String item : itemSet) {
				if (!nodesMap.containsKey(item)) {
					nodesMap.put(item, icd9Map.get(item));
				}
				sb.append("," + icd9Map.get(item));
			}

			// if (itemSet.length > 1) {
			// if (!edgeMap.containsKey(itemSet[0])) {
			// edgeMap.put(itemSet[0], new HashSet<String>());
			// }
			// for (int i = 1; i < itemSet.length; i++) {
			// edgeMap.get(itemSet[0]).add(itemSet[i]);
			// }
			// }
			if (itemSet.length > 1) {
				collector.printf("%-20s %.30s(%s)\n", key, sb.substring(1), pair.getSecond());
			}

			// for (String item : itemSet) {
			// sb.append("," + item + ":{'color':'red','shape':'dot','label':'" + icd9Map.get(item) + "'},");
			// }
			// System.out.println(sb.substring(1));
			// if (itemSet.length > 1) {
			// StringBuilder tmp = new StringBuilder();
			// for (int i = 1; i < itemSet.length; i++) {
			// tmp.append("," + itemSet[i] + ":{}");
			// }
			//
			// collector.printf("node : %s:{ %s }", itemSet[0], tmp.substring(1));
			// }
		}
	}
}