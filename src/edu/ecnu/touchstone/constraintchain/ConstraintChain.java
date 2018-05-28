package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ConstraintChain implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String tableName = null;
	private List<CCNode> nodes = null;

	public ConstraintChain(String tableName, List<CCNode> nodes) {
		super();
		this.tableName = tableName;
		this.nodes = nodes;
	}
	
	public ConstraintChain(ConstraintChain constraintChain) {
		super();
		this.tableName = constraintChain.tableName;
		this.nodes = new ArrayList<CCNode>();
		for (int i = 0; i < constraintChain.nodes.size(); i++) {
			this.nodes.add(new CCNode(constraintChain.nodes.get(i)));
		}
	}

	public String getTableName() {
		return tableName;
	}
	
	public List<CCNode> getNodes() {
		return nodes;
	}

	@Override
	public String toString() {
		return "\nConstraintChain [tableName=" + tableName + ", nodes=" + nodes + "]";
	}
}
