package com.sky.mahout;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.common.iterator.StringRecordIterator;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPGrowth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	private static String input = "";

	public static void main(String[] args) throws IOException {

		StopWatch stopWatch = new StopWatch();

		stopWatch.reset();
		stopWatch.start();
		FPGrowth<String> fp = new FPGrowth<String>();
		Set<String> features = new HashSet<String>();

		String pattern = " \"[ ,\\t]*[,|\\t][ ,\\t]*\" ";
		FileLineIterable fileLineIterable = new FileLineIterable(new File(input));
		StringRecordIterator stringRecordIterator = new StringRecordIterator((fileLineIterable), pattern);

		long minSupport = 10;
		int maxHeapSize = 100;
		// @formatter:off
		fp.generateTopKFrequentPatterns(
				stringRecordIterator, 
				fp.generateFList(stringRecordIterator, new Long(minSupport).intValue()),
				minSupport, 
				maxHeapSize, 
				features, 
				new PrintStreamConverter(System.out), 
				new ContextStatusUpdater(null));
		// @formatter:on
		stopWatch.stop();
		logger.info("{}", stopWatch);

		logger.info("{}", features.toString());
	}

}

class PrintStreamConverter implements OutputCollector<String, List<Pair<List<String>, Long>>> {
	private final PrintStream collector;
	private static String ICD9_FILE_PATH = "";
	private HashMap<String, String> icd9Map = new HashMap<String, String>();

	public PrintStreamConverter(PrintStream collector) {
		this.collector = collector;
		List<String> icd9List = null;
		try {
			icd9List = FileUtils.readLines(new File(ICD9_FILE_PATH));
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
			collector.print(key + ": " + sb.substring(1) + "\t" + pair.getSecond() + "\n");
		}
	}
}