package edu.ecnu.touchstone.schema;

// mixed reference is not supported in the definition of schema currently
// in the definition of cardinality constraints, we have supported the mixed reference
public class ForeignKey {
	
	private String attrName = null;
	
	// include the referenced table name
	private String referencedKey = null;
	
	public ForeignKey(String attrName, String referencedKey) {
		super();
		this.attrName = attrName;
		this.referencedKey = referencedKey;
	}

	public String getAttrName() {
		return attrName;
	}

	public String getReferencedKey() {
		return referencedKey;
	}

	@Override
	public String toString() {
		return "ForeignKey [attrName=" + attrName + ", referencedKey=" + referencedKey + "]";
	}
}
