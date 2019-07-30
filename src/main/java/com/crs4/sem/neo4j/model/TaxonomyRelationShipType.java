package com.crs4.sem.neo4j.model;

import org.neo4j.graphdb.RelationshipType;

public enum TaxonomyRelationShipType implements RelationshipType{
IS_SON_OF, HAS_KEYWORDS, HAS_DOCUMENT, IS_PARENT_OF
}
