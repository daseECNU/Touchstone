package edu.ecnu.touchstone.constraintchain;

import java.util.Arrays;
import java.util.List;

// The adjustment of 'FKJoin' node (to solve the 'special situation')
// special situation: it does not exist some combined join statuses of the referenced primary key

public class FKJoinAdjustment {
	
	// the order of the 'FKJoin' node in all constraint chains
	private int order;
	
	// record the join status for all 'FKJoin' node of this foreign key
	// all 'FKJoinAdjustment' only share one array 'joinStatuses'
	private boolean[] joinStatuses = null;

	// we avoid to generate a non-existent combined join statuses for the foreign key by adding 
	// 'rules' according to the join information of the referenced primary key
	private List<FKJoinAdjustRule> rules = null;
	
	// after adjustment, the 'probability' of can-join situation (join status is True)
	private float probability;

	public FKJoinAdjustment(int order, boolean[] joinStatuses, List<FKJoinAdjustRule> rules, float probability) {
		super();
		this.order = order;
		this.joinStatuses = joinStatuses;
		this.rules = rules;
		this.probability = probability;
	}

	public List<FKJoinAdjustRule> getRules() {
		return rules;
	}

	public float getProbability() {
		return probability;
	}

	public boolean canJoin() {
		loop : for (int i = 0; i < rules.size(); i++) {
			boolean[] cause = rules.get(i).getCause();
			for (int j = 0; j < cause.length; j++) {
				if (cause[j] != joinStatuses[j]) {
					continue loop;
				}
			}
			return joinStatuses[order] = rules.get(i).getEffect();
		}

		if (Math.random() < probability) {
			return joinStatuses[order] = true;
		} else {
			return joinStatuses[order] = false;
		}
	}

	@Override
	public String toString() {
		return "FKJoinAdjustment [order=" + order + ", joinStatuses=" + Arrays.toString(joinStatuses) + 
				", rules=" + rules + ", probability=" + probability + "]";
	}
}


