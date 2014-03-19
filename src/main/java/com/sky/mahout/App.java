package com.sky.mahout;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.common.iterator.StringRecordIterator;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPGrowth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException {

		StopWatch stopWatch = new StopWatch();

		stopWatch.reset();
		stopWatch.start();
		FPGrowth<String> fp = new FPGrowth<String>();
		Set<String> features = new HashSet<String>();

		String pattern = "[,]";
		FileLineIterable fileLineIterable = new FileLineIterable(new File(AppConfig.INPUT_FILE_PATH));
		StringRecordIterator stringRecordIterator = new StringRecordIterator((fileLineIterable), pattern);

		long minSupport = 5;
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

//		PrintStreamConverter.print();
	}
}
