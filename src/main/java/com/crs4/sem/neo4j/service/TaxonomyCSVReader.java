package com.crs4.sem.neo4j.service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.crs4.sem.neo4j.exceptions.CategoryNotFoundException;
import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

public class TaxonomyCSVReader {

	public static Logger log = Logger.getLogger(TaxonomyCSVReader.class.getSimpleName());

	public static void read(File file, TaxonomyService service) throws IOException {
		InputStream inputStream = new FileInputStream(file);
		readIStream(inputStream, service);
	}

	public static void readIStream(InputStream inputStream, TaxonomyService service) throws IOException {
		
		//InputStreamReader in = new InputStreamReader(inputStream);
		BufferedInputStream bis = new BufferedInputStream(inputStream);
		CharsetDetector cd = new CharsetDetector();
		cd.setText(bis);
		CharsetMatch cm = cd.detect();

		if (cm != null) {
		   Reader reader = cm.getReader();
		   String charset = cm.getName();
		

		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter(';').parse(reader);

		for (CSVRecord record : records) {
			String parent = record.get(0).toLowerCase().trim();
			String child = record.get(1).toLowerCase().trim();
			service.addToParent(parent, child);
			// System.out.println("added " + parent + "->" + child);
		}
		}else {
			   throw new UnsupportedCharsetException("");
			}
	}

	public static void readKeywordsStream(InputStream inputStream, TaxonomyService service) throws IOException {
		InputStreamReader in = new InputStreamReader(inputStream);
		BufferedInputStream bis = new BufferedInputStream(inputStream);
		CharsetDetector cd = new CharsetDetector();
		cd.setText(bis);
		CharsetMatch cm = cd.detect();

		if (cm != null) {
		   Reader reader = cm.getReader();
		   String charset = cm.getName();
		
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter(';').parse(reader);

		for (CSVRecord record : records) {
			String category = record.get(0).toLowerCase().trim();
			String keyword = record.get(1).toLowerCase().trim();
			try {
				service.addKeyword(category, keyword);
			} catch (CategoryNotFoundException e) {
				log.log(Level.SEVERE, "category not found", e);
			}

		}
		}else {
			   throw new UnsupportedCharsetException("");
			}

	}

	public static void readTriple(InputStream inputStream, TaxonomyService service) throws IOException {

		BufferedInputStream bis = new BufferedInputStream(inputStream);
		CharsetDetector cd = new CharsetDetector();
		cd.setText(bis);
		CharsetMatch cm = cd.detect();

		if (cm != null) {
		   Reader reader = cm.getReader();
		   String charset = cm.getName();
		
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter(';').withSkipHeaderRecord().parse(reader);
		Set<String> categories_level1 = new HashSet<String>();
		Set<String> categories_level2 = new HashSet<String>();
		int i = 0;
		for (CSVRecord record : records) {
			if (i != 0) {
				String parent = record.get(0).toLowerCase().trim();
				String child = record.get(1).toLowerCase().trim();
				String keyword = record.get(2).trim();
				if (!categories_level1.contains(parent))
					service.addToParent("root", parent);
				if (!categories_level2.contains(child))
					service.addToParent(parent, parent+"_"+child);
				try {
					service.addKeyword(parent+"_"+child, keyword);
				} catch (CategoryNotFoundException e) {
					log.log(Level.WARNING, "", e);
				}
				System.out.println("added " + parent + "->" + child);
			}
			i++;
		}
}else {
	   throw new UnsupportedCharsetException("");
	}

	}
}
