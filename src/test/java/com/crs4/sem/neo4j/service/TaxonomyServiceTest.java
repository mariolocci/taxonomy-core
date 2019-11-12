package com.crs4.sem.neo4j.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileUtils;

import com.crs4.sem.neo4j.exceptions.CategoryNotFoundInTaxonomyException;
import com.crs4.sem.neo4j.exceptions.TaxonomyNotFoundException;
import com.crs4.sem.neo4j.model.CategoryNode;
import com.crs4.sem.neo4j.model.RRelationShipType;

public class TaxonomyServiceTest {
	
	
	@Test
	public void testaddCategory(){
		String directory="/tmp/";
		TaxonomyService taxoservice = new TaxonomyService( new File(directory));
		Node cat=null;
		if( taxoservice.searchCategory("Root")==null)
		    cat=taxoservice.createCategory("Root");
		
		Node node = taxoservice.searchCategory("Root");
		assertNotNull(node);
	}

	
	@Test
	public void testBranchLabel() throws IOException{
		
		   File neodirectory = new File("/tmp/test");
		   if(neodirectory.exists())
			   FileUtils.deleteRecursively(neodirectory);;
		   TaxonomyService taxoservice = new TaxonomyService(new File("/tmp/test"));
		   TaxonomyCSVReader.read(new File("src/test/resources/taxo.csv"), taxoservice);
		   Node node=taxoservice.searchCategory("root");
		   String labels[] =taxoservice.branchLabels(node);
		   Arrays.sort(labels);
		   System.out.println(Arrays.toString(labels));
		   node=taxoservice.searchCategory("arte");
		   labels =taxoservice.branchLabels(node);
		   Arrays.sort(labels);
		   System.out.println(Arrays.toString(labels));
		assert(Arrays.binarySearch(labels, "root")<0);
	}
	
	@Test
	public void testTriple() throws IOException, CategoryNotFoundInTaxonomyException{
		
		   TaxonomyService taxoservice = buildTaxonomy();
		   Node node=taxoservice.searchCategory("root");
		   String labels[] =taxoservice.branchLabels(node);
		   Arrays.sort(labels);
		   System.out.println(Arrays.toString(labels));
		   node=taxoservice.searchCategory("diritti civili");
		   labels =taxoservice.branchLabels(node);
		   Arrays.sort(labels);
		   System.out.println(Arrays.toString(labels));
		assert(Arrays.binarySearch(labels, "root")<0);
		String[] keywords = taxoservice.getKetwords("root", "imprese");
		assertEquals(44,keywords.length,0);
	}


	private TaxonomyService buildTaxonomy() throws IOException, FileNotFoundException {
		File neodirectory = new File("/tmp/test");
		   if(neodirectory.exists())
			   FileUtils.deleteRecursively(neodirectory);;
		   TaxonomyService taxoservice = new TaxonomyService(new File("/tmp/test"));
		   TaxonomyCSVReader.readTriple(new FileInputStream(new File("src/test/resources/SOS_181202_Tassonomia_rev1.csv")), taxoservice);
		return taxoservice;
	}
	
@Test	
	public void testDelete() throws TaxonomyNotFoundException, IOException {
		  TaxonomyService taxoservice = buildTaxonomy();
		   Node node=taxoservice.searchCategory("root");
		   String [] categories=taxoservice.branchLabels(node);
		   assertNotNull(node);
		   taxoservice.deleteTaxonomy("root");
		    node=taxoservice.searchCategory("root");
		   assertNull(node);
		   TaxonomyCSVReader.readTriple(new FileInputStream(new File("src/test/resources/SOS_181202_Tassonomia_rev1.csv")), taxoservice);
		   node=taxoservice.searchCategory("root");
		   assertNotNull(node);
		   String [] categories2=taxoservice.branchLabels(node);
		   assertEquals(categories.length,categories2.length,0);
		
	}

@Test	
	public void getBranch() throws FileNotFoundException, IOException {
	 TaxonomyService taxonomyService = buildTaxonomy();

		CategoryNode categoryNode=taxonomyService.getBranch("root", "root");
		assertNotNull(categoryNode);
	}

@Test 
public void getKeywords() throws FileNotFoundException, IOException, TaxonomyNotFoundException, CategoryNotFoundInTaxonomyException {
	 TaxonomyService taxonomyService = buildTaxonomy();

		Set<String> keywords = taxonomyService.getAllKeywords("root", true);
		assertEquals(keywords.size(),900);
		
}

}

