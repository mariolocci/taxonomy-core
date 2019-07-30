package com.crs4.sem.neo4j.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.crs4.sem.neo4j.model.RRelationShipType;
import com.crs4.sem.neo4j.model.MyLabels;
import com.crs4.sem.neo4j.model.TaxonomyRelationShipType;

import lombok.Data;

@Data
public class NodeService {

	private GraphDatabaseService graphDb;

	public NodeService(File neo4jdirectory) {
		this.setGraphDb(new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(neo4jdirectory)
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
				.setConfig(GraphDatabaseSettings.string_block_size, "60")
				.setConfig(GraphDatabaseSettings.array_block_size, "300").newGraphDatabase());

	}

	public NodeService(GraphDatabaseService graphDb) {
		this.setGraphDb(graphDb);
	}

	public Node createNode(String label, String property, String value) {
		Node node = null;
		try (Transaction tx = graphDb.beginTx()) {
			node = this.getGraphDb().createNode();
			node.addLabel(Label.label(label));
			node.setProperty(property, value);
			IndexManager indexManager = this.getGraphDb().index();
			Index<Node> catindex = indexManager.forNodes(label);
			catindex.add(node, property, value);

			tx.success();
		}
		return node;
	}

	public void addRelationShip(Node from, Node to, RRelationShipType relationship) {
		try (Transaction tx = graphDb.beginTx()) {
			from.createRelationshipTo(to, relationship);
			tx.success();
		}
	}

	public void delete(Node node) {
		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Relationship> relations = node.getRelationships();
			for (Relationship each : relations) {
				each.delete();
			}
			node.delete();
			tx.success();
		}

	}

	public Node searchNode(String label, String property, String value) {
		Node node = null;
		try (Transaction tx = graphDb.beginTx()) {
			IndexHits<Node> nodes = nodes(label, property, value);
			node = nodes.getSingle();
			tx.success();
		}
		return node;

	}

	public List<Node> searchNodes(String label, String property, String value) {
		List<Node> results = new ArrayList<Node>();
		try (Transaction tx = graphDb.beginTx()) {
			IndexHits<Node> nodes = nodes(label, property, value);
			while (nodes.hasNext())
				results.add(nodes.next());
			tx.success();
		}
		return results;
	}

	public IndexHits<Node> nodes(String label, String property, String value) {
		IndexManager indexManager = this.getGraphDb().index();
		Index<Node> catindex = indexManager.forNodes(label);
		IndexHits<Node> nodes = catindex.get(property, value);
		return nodes;
	}

	public List<Node> getBreadthFirstNodes(Node root, RRelationShipType relationship, int depth) {
		List<Node> result = new ArrayList<Node>();
		try (Transaction tx = graphDb.beginTx()) {
			TraversalDescription traversal = this.getGraphDb().traversalDescription();
			for (Node node : traversal.breadthFirst().relationships(relationship, Direction.OUTGOING)
					.evaluator(Evaluators.toDepth(depth)).traverse(root)

					.nodes()) {
				if (!node.equals(root))
					result.add(node);
			}
			tx.success();
		}
		return result;
	}

	
	
	public List<Node> visita(Node root, RRelationShipType relationship, boolean rootincluded) {
		List<Node> result = new ArrayList<Node>();
		try (Transaction tx = graphDb.beginTx()) {
			TraversalDescription traversal = this.getGraphDb().traversalDescription();
			for (Node node : traversal.breadthFirst().relationships(relationship, Direction.OUTGOING)

					.evaluator(rootincluded ? Evaluators.all() : Evaluators.excludeStartPosition()).traverse(root)
					.nodes()) {
				result.add(node);
			}
			tx.success();
		}
		return result;
	}

	public String getCategory(Node node) {
		String label = null;
		try (Transaction tx = graphDb.beginTx()) {
			label = node.getProperty("name").toString();
			tx.success();
		}
		return label;
	}

	public String getKeyword(Node node) {
		String label = null;
		try (Transaction tx = graphDb.beginTx()) {
			label = node.getProperty("keyword_id").toString();
			tx.success();
		}
		return label;
	}
	public void setProperty(Node nodedoc, String key, Long value) {
		try (Transaction tx = graphDb.beginTx()) {
			nodedoc.setProperty(key, value);
			tx.success();
		}
	}

	public List<Node> getLinkedNodes(Node node, RRelationShipType type) {
		List<Node> nodes = new ArrayList<Node>();
		try (Transaction tx = graphDb.beginTx()) {
			Iterable<Relationship> iter = node.getRelationships();

			for (Relationship rel : iter) {

				Node endnode = rel.getEndNode();
				if (rel.getType().name().equals(type.name()))
					nodes.add(endnode);
			}
			
			tx.success();
			return nodes;
		}
	}
}
