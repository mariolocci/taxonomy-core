package com.crs4.sem.neo4j.model;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CategoryNode {
	public String name;
	public List<CategoryNode> children;

}
