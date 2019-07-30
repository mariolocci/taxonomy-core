package com.crs4.sem.neo4j.service;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.io.fs.FileUtils;

public class TaxonomyCSVReaderTest {
	
   @Test 	
	public void read() throws IOException{
	   //TaxonomyCSVReader taxoreader = new TaxonomyCSVReader();
	   File neodirectory = new File("/tmp/test");
	   if(neodirectory.exists())
		   FileUtils.deleteRecursively(neodirectory);;
	   TaxonomyService taxoservice = new TaxonomyService(new File("/tmp/test"));
	   TaxonomyCSVReader.read(new File("src/test/resources/taxo.csv"), taxoservice);
	   assertNotNull( taxoservice.searchCategory("musica")); 
   }

   
   @Test 	
  	public void readIStream() throws IOException{
  	   //TaxonomyCSVReader taxoreader = new TaxonomyCSVReader();
  	   File neodirectory = new File("/tmp/test");
  	   if(neodirectory.exists())
  		   FileUtils.deleteRecursively(neodirectory);;
  	   TaxonomyService taxoservice = new TaxonomyService(new File("/tmp/test"));
  	   TaxonomyCSVReader.readIStream(new FileInputStream(new File("src/test/resources/taxo.csv")), taxoservice);
  	   assertNotNull( taxoservice.searchCategory("musica")); 
     }
}
