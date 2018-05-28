package edu.ecnu.touchstone.datatype;

public class TSReal implements TSDataTypeInfo {
	
	private static final long serialVersionUID = 1L;
	
	protected float nullRatio;
	protected double min;
	protected double max;

	public TSReal() {
		super();
		nullRatio = 0;
		min = -100000000;
		max = 100000000;
	}

	public TSReal(float nullRatio, double min, double max) {
		super();
		this.nullRatio = nullRatio;
		this.min = min;
		this.max = max;
	}
	
	public TSReal(TSReal tsReal) {
		super();
		this.nullRatio = tsReal.nullRatio;
		this.min = tsReal.min;
		this.max = tsReal.max;
	}
	
	public Double geneData() {
		if (Math.random() < nullRatio) {
			return null;
		}
		return Math.random() * (max - min) + min;
	}

	@Override
	public String toString() {
		return "TSReal [nullRatio=" + nullRatio + ", min=" + min + ", max=" + max + "]";
	}

	@Override
	public double getMinValue() {
		return min;
	}

	@Override
	public double getMaxValue() {
		return max;
	}
}
