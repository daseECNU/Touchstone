package edu.ecnu.touchstone.constraintchain;

import java.util.Arrays;

// The adjust-rule for 'FKJoinAdjustment'
public class FKJoinAdjustRule implements Comparable<FKJoinAdjustRule> {
	
	private boolean[] cause = null;
	private boolean effect;

	// the input parameter 'joinStatuses' is a non-existent combined join statuses
	public FKJoinAdjustRule(boolean[] joinStatuses) {
		super();
		cause = Arrays.copyOf(joinStatuses, joinStatuses.length - 1);
		effect = !joinStatuses[joinStatuses.length - 1];
	}
	
	public boolean[] getCause() {
		return cause;
	}

	public boolean getEffect() {
		return effect;
	}

	@Override
	public String toString() {
		return "FKJoinAdjustRule [cause=" + Arrays.toString(cause) + ", effect=" + effect + "]";
	}

	// support for removing invalid rules (there may exist two rules where they have same cases and different effects)
	@Override
	public int compareTo(FKJoinAdjustRule other) {
		return Arrays.toString(other.cause).compareTo(Arrays.toString(this.cause));
	}
}