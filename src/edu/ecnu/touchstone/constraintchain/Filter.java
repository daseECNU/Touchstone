package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

// Filter node of a constraint chain
// pattern: [0, exp1@op1#exp2@op2 ... #and|or, probability]
public class Filter implements Serializable {

	private static final long serialVersionUID = 1L;

	// there may be multiple basic filter operations
	private FilterOperation[] filterOperations = null;
	// The logical relation between multiple basic filter operations
	// -1: default value (there is only one basic filter operations), 0: and, 1: or
	private int logicalRelation;
	// the 'probability' is the combined probability of multiple basic filter operations, and the probability
	// of each basic filter operation can be calculated based on it
	private float probability;

	public Filter(FilterOperation[] filterOperations, int logicalRelation, float probability) {
		super();
		this.filterOperations = filterOperations;
		this.logicalRelation = logicalRelation;
		this.probability = probability;
	}

	public Filter(Filter filter) {
		super();
		this.filterOperations = new FilterOperation[filter.filterOperations.length];
		for (int i = 0; i < filter.filterOperations.length; i++) {
			this.filterOperations[i] = new FilterOperation(filter.filterOperations[i]);
		}
		this.logicalRelation = filter.logicalRelation;
		this.probability = filter.probability;
	}

	public boolean isSatisfied(Map<String, String> attributeValueMap) {
		boolean res = false;
		if (logicalRelation == -1) { // only one basic filter operation
			res = filterOperations[0].isSatisfied(attributeValueMap);
		} else if (logicalRelation == 0) { // and
			res = true;
			for (int i = 0; i < filterOperations.length; i++) {
				if (!filterOperations[i].isSatisfied(attributeValueMap)) {
					res = false;
					break;
				}
			}
		} else if (logicalRelation == 1) { // or
			res = false;
			for (int i = 0; i < filterOperations.length; i++) {
				if (filterOperations[i].isSatisfied(attributeValueMap)) {
					res = true;
					break;
				}
			}
		}
		return res;
	}

	public FilterOperation[] getFilterOperations() {
		return filterOperations;
	}

	public int getLogicalRelation() {
		return logicalRelation;
	}

	public float getProbability() {
		return probability;
	}

	@Override
	public String toString() {
		return "\n\tFilter [filterOperations=" + Arrays.toString(filterOperations) + ", \n\t\tlogicalRelation=" 
				+ logicalRelation + ", probability=" + probability + "]";
	}
}