package com.crs4.sem.neo4j.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import com.crs4.sem.neo4j.exceptions.CategoryNotFoundException;
import com.crs4.sem.neo4j.exceptions.CategoryNotFoundInTaxonomyException;
import com.crs4.sem.neo4j.exceptions.TaxonomyNotFoundException;
import com.crs4.sem.neo4j.model.CategoryNode;
import com.crs4.sem.neo4j.model.MyLabels;
import com.crs4.sem.neo4j.model.RRelationShipType;

import lombok.Data;

@Data
public class TaxonomyService extends NodeService {

	// private NodeService nodeService;

	public TaxonomyService(File neo4jdirectory) {
		super(neo4jdirectory);
	}

	public TaxonomyService(GraphDatabaseService graphDb) {
		super(graphDb);
	}

	public Node createCategory(String category_name) {
		return this.createNode(MyLabels.CATEGORY.toString(), "name", category_name);

	}

	public Node createDocument(String document_id) {
		return this.createNode(MyLabels.DOCUMENT.toString(), "document_id", document_id);

	}

	public Node createKeyword(String keyword_id) {
		return this.createNode(MyLabels.KEYWORD.toString(), "keyword_id", keyword_id);

	}

	public void addToParent(Node parent, Node category) {
		this.addRelationShip(parent, category, RRelationShipType.IS_PARENT_OF);
		this.addRelationShip(category, parent, RRelationShipType.IS_SON_OF);
	}

	public void addDocument(Node category, Node document) {
		this.addRelationShip(category, document, RRelationShipType.HAS_DOCUMENT);

	}

	public void addKeyword(Node category, Node keyword) {
		this.addRelationShip(category, keyword, RRelationShipType.HAS_KEYWORD);

	}

	public List<Node> getKeywords(Node category) {
		return this.getLinkedNodes(category, RRelationShipType.HAS_KEYWORD);

	}

	public Node searchCategory(String category_name) {
		return this.searchNode(MyLabels.CATEGORY.toString(), "name", category_name);

	}

	public void addToParent(String parent, String category) {
		Node category_node = null;
		Node parent_node = null;
		category_node = this.searchCategory(category);
		if (category_node == null) {
			category_node = this.createCategory(category);
		}
		parent_node = this.searchCategory(parent);
		if (parent_node == null) {
			parent_node = this.createCategory(parent);
		}

		this.addToParent(parent_node, category_node);
	}

	public List<Node> getChildren(Node root) {
		return this.getBreadthFirstNodes(root, RRelationShipType.IS_PARENT_OF, 1);

	}

	public String[] branchLabels(Node child) {

		List<Node> nodes = this.visita(child, RRelationShipType.IS_PARENT_OF, false);
		String[] results = new String[nodes.size()];

		for (int i = 0; i < nodes.size(); i++)
			results[i] = this.getCategory(nodes.get(i));
		return results;
	}

	public String[] branchLabels(Node child, boolean rootincluded) {
		List<Node> nodes = this.visita(child, RRelationShipType.IS_PARENT_OF, rootincluded);
		String[] results = new String[nodes.size()];

		for (int i = 0; i < nodes.size(); i++)
			results[i] = this.getCategory(nodes.get(i));
		return results;
	}
	
	

	
	public CategoryNode getBranch(String taxoname, String category_id) {
		Queue<CategoryNode> queue= new LinkedList<CategoryNode>();
		
		CategoryNode head = CategoryNode.builder().name(category_id).build();
		
		queue.add(head);
		while(!queue.isEmpty()) {
			CategoryNode parent=queue.remove();
			Node root= this.searchCategory(parent.getName());
			List<Node> children=this.getChildren(root);
			List<CategoryNode> categories=null;
			if(!children.isEmpty())
				 categories=new ArrayList<CategoryNode>();
			for(Node chil:children) {
				String id=this.getCategory(chil);
				CategoryNode current=CategoryNode.builder().name(id).build();
				queue.add(current);
				categories.add(current);	
			}
			parent.setChildren(categories);
		}
			return head;
		
	}
	public void addKeyword(String category, String keyword) throws CategoryNotFoundException {
		Node category_node = null;
		Node keyword_node = null;
		category_node = this.searchCategory(category);
		if (category_node == null) {
			throw new CategoryNotFoundException();
		}
		keyword_node = this.searchKeyword(keyword);
		if (keyword_node == null) {
			keyword_node = this.createKeyword(keyword);
		}

		this.addKeyword(category_node, keyword_node);

	}

	public Node searchKeyword(String keyword) {
		return this.searchNode(MyLabels.KEYWORD.toString(), "name", keyword);
	}

	public void deleteTaxonomy(String name) throws TaxonomyNotFoundException {
		Node node_root = this.searchNode(MyLabels.CATEGORY.toString(), "name", name);
		if (node_root == null)
			throw new TaxonomyNotFoundException();
		List<Node> nodes = this.visita(node_root, RRelationShipType.IS_PARENT_OF, true);
		this.deleteKeywords(node_root);
		for (int i = 0; i < nodes.size(); i++) {
			this.deleteCategory(nodes.get(i));
		}

	}

	private void deleteCategory(Node node) {
		try (Transaction tx = this.getGraphDb().beginTx()) {
			IndexManager indexManager = this.getGraphDb().index();
			Index<Node> catindex = indexManager.forNodes(MyLabels.CATEGORY.toString());
			catindex.remove(node);

			Iterable<Relationship> relations = node.getRelationships();
			for (Relationship each : relations) {
				each.delete();
			}
			node.delete();

			tx.success();
		}
	}

	public void deleteKeywords(Node node_root) {
		List<Node> nodes = this.visita(node_root, RRelationShipType.IS_PARENT_OF, true);
		for (int i = 0; i < nodes.size(); i++) {
			List<Node> keywords = this.getKeywords(nodes.get(i));
			for (Node keyword : keywords)
				this.delete(keyword);
		}

	}

	public String[] getKetwords(String root, String category) throws CategoryNotFoundInTaxonomyException {
		if (!this.categoryHasRoot(root, category))
			throw new CategoryNotFoundInTaxonomyException();
		Node category_node = this.searchCategory(category);
		List<Node> keywords_nodes = this.getKeywords(category_node);
		if (keywords_nodes.isEmpty())
			return null;
		String[] result = new String[keywords_nodes.size()];
		for (int i = 0; i < keywords_nodes.size(); i++) {
			result[i] = this.getKeyword(keywords_nodes.get(i));
		}
		return result;
	}
	
	public Set<String> getAllKeywords(String name, boolean lowercase) throws TaxonomyNotFoundException, CategoryNotFoundInTaxonomyException {
		Node root=this.searchCategory(name);
		if(root==null) throw new com.crs4.sem.neo4j.exceptions.TaxonomyNotFoundException();
	    String[] labels = this.branchLabels(root, true);
	    Set<String> keywords= new HashSet<String>();
	    for( String label:labels) {
	    	
	        String[] klist = (this.getKetwords(name, label));
	 
	    	if(klist!=null&&klist.length>0) {
	    		if(lowercase) 
	    			for(int j=0;j<klist.length;j++)
	    				klist[j]=klist[j].toLowerCase();
	    		keywords.addAll( Arrays.asList(klist ));
	    	}
	    }
		return keywords;
	}

	public boolean categoryHasRoot(String root, String category) {
		// TODO Auto-generated method stub
		return true;
	}

}
