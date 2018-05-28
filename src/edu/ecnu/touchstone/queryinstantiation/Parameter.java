package edu.ecnu.touchstone.queryinstantiation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Parameter implements Comparable<Parameter>, Serializable {

	private static final long serialVersionUID = 1L;
	
	private int id;
	private List<String> values = null;
	private long cardinality;
	private long deviation;
	
	// support non-equi join
	private boolean isBet;
	private String constraint = null;

	public Parameter(int id, List<String> values, long cardinality, long deviation) {
		super();
		this.id = id;
		this.values = values;
		this.cardinality = cardinality;
		this.deviation = deviation;
	}
	
	public Parameter(Parameter para) {
		super();
		this.id = para.id;
		this.values = new ArrayList<String>();
		this.values.addAll(para.values);
		this.cardinality = para.cardinality;
		this.deviation = para.deviation;
		this.isBet = para.isBet;
		this.constraint = para.constraint;
	}
	
	public Parameter(int id, List<String> values, long cardinality, long deviation, boolean isBet, String constraint) {
		super();
		this.id = id;
		this.values = values;
		this.cardinality = cardinality;
		this.deviation = deviation;
		this.isBet = isBet;
		this.constraint = constraint;
	}

	public void merge(Parameter para) {
		values.add(para.values.get(0));
		double value1 = Double.parseDouble(values.get(0));
		double value2 = Double.parseDouble(para.values.get(0));
		if (value1 > value2) {
			Collections.reverse(values);
		}
		deviation = (deviation + para.deviation) / 2;
		constraint = constraint + " && " + para.constraint;
	}

	public int getId() {
		return id;
	}

	public List<String> getValues() {
		return values;
	}

	public long getCardinality() {
		return cardinality;
	}

	public long getDeviation() {
		return deviation;
	}

	public boolean isBet() {
		return isBet;
	}

	public String getConstraint() {
		return constraint;
	}

	@Override
	public String toString() {
		return "\n\tParameter [id=" + id + ", values=" + values + ", cardinality=" + cardinality + ", deviation="
				+ deviation + "]";
	}

	@Override
	public int compareTo(Parameter other) {
		if (this.id < other.id) {
			return -1;
		} else {
			return 1;
		}
	}
}
