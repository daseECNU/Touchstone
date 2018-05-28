package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;

// The node of the constraint chain
public class CCNode implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// the type of the node: 0, Filter; 1, PKJoin; 2, FKJoin
	private int type;
	
	private Object node = null;

	public CCNode(int type, Object node) {
		super();
		this.type = type;
		this.node = node;
	}

	public CCNode(CCNode ccNode) {
		super();
		this.type = ccNode.type;
		switch (this.type) {
		case 0:
			this.node = new Filter((Filter)ccNode.node);
			break;
		case 1:
			this.node = new PKJoin((PKJoin)ccNode.node);
			break;
		case 2:
			this.node = new FKJoin((FKJoin)ccNode.node);
			break;
		}
	}

	public int getType() {
		return type;
	}

	public Object getNode() {
		return node;
	}

	@Override
	public String toString() {
		return node.toString();
	}
}
