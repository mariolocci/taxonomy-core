package com.crs4.sem.neo4j.model;

import org.neo4j.graphdb.RelationshipType;

public enum RRelationShipType implements RelationshipType{
CONTAINS,IS_SON_OF, HAS_KEYWORD, HAS_DOCUMENT, IS_PARENT_OF
}
