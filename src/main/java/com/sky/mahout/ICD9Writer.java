package com.sky.mahout;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class ICD9Writer {

	private static final Logger logger = LoggerFactory.getLogger(ICD9Writer.class);

	public static Map<String, Set<String>> getAdmissionData(Map<String, Set<String>> dataMap, String mdbPath, String year) {
		if (dataMap == null) {
			dataMap = new HashMap<String, Set<String>>();
		}

		Table table = null;
		try {
			table = DatabaseBuilder.open(new File(mdbPath)).getTable("R201_DD" + year);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Rows : {}", table.getRowCount());
		int insertCount = 0;
		for (Row row : table) {
			if (row.get("ID") == null) {
				continue;
			}
			String ID = String.valueOf(row.get("ID"));

			Set<String> bucket = null;
			if (!dataMap.containsKey(ID)) {
				bucket = new TreeSet<String>();
				dataMap.put(ID, bucket);
				insertCount++;
			} else {
				bucket = dataMap.get(ID);
			}

			for (int i = 0; i < 5; i++) {
				String columnName = "ICD9CM_CODE" + (i == 0 ? "" : "_" + String.valueOf(i));
				if (row.get(columnName) != null) {
					bucket.add(String.valueOf(row.get(columnName)));
				} else {
					break;
				}
			}
		}
		logger.info("Insert Rows : {}", insertCount);
		return dataMap;
	}

	public static Map<String, Set<String>> getOutPatientClinicData(Map<String, Set<String>> dataMap, String mdbPath, String year) {
		if (dataMap == null) {
			dataMap = new HashMap<String, Set<String>>();
		}

		Table table = null;
		try {
			table = DatabaseBuilder.open(new File(mdbPath)).getTable("R201_CD" + year);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("Rows : {}", table.getRowCount());
		int insertCount = 0;
		for (Row row : table) {
			if (row.get("ID") == null) {
				continue;
			}
			String ID = String.valueOf(row.get("ID"));

			Set<String> bucket = null;
			if (!dataMap.containsKey(ID)) {
				bucket = new TreeSet<String>();
				dataMap.put(ID, bucket);
				insertCount++;
			} else {
				bucket = dataMap.get(ID);
			}

			for (int i = 1; i < 4; i++) {
				String columnName = "ACODE_ICD9_" + i;

				if (row.get(columnName) != null) {
					bucket.add(String.valueOf(row.get(columnName)));
				} else {
					break;
				}
			}
		}
		logger.info("Insert Rows : {}", insertCount);
		return dataMap;
	}

	public static void main(String[] args) throws IOException {

		HashMap<String, Set<String>> dataMap = new HashMap<String, Set<String>>();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		getAdmissionData(dataMap, AppConfig.MDB1_FILE_PATH, "2005");
		getOutPatientClinicData(dataMap, AppConfig.MDB1_FILE_PATH, "2005");
		getAdmissionData(dataMap, AppConfig.MDB1_FILE_PATH, "2006");
		getOutPatientClinicData(dataMap, AppConfig.MDB1_FILE_PATH, "2006");
		stopWatch.stop();
		logger.info("2005 ~ 2006 , data size : {} - {}", dataMap.size(), stopWatch);
		stopWatch.reset();
		stopWatch.start();
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2000");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2001");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2002");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2003");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2004");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2007");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2008");
		getOutPatientClinicData(dataMap, AppConfig.MDB2_FILE_PATH, "2009");
		stopWatch.stop();
		logger.info("2000 ~ 2004 , data size : {} - {}", dataMap.size(), stopWatch);
		stopWatch.reset();
		stopWatch.start();
		FileWriter datWriter = new FileWriter(AppConfig.INPUT_FILE_PATH);
		int transactionCount = 0;
		for (Iterator<Set<String>> itr = dataMap.values().iterator(); itr.hasNext();) {
			Set<String> itemSet = itr.next();
			if (itemSet.size() > 1) {
				datWriter.append(StringUtils.join(itemSet.toArray(new String[itemSet.size()]), ","));
				datWriter.append("\n");
				transactionCount++;
			}
		}
		datWriter.close();
		stopWatch.stop();
		logger.info("transactionCount : {}", transactionCount);
		logger.info("{}", stopWatch);

	}
}
