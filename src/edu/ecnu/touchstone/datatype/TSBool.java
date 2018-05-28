package edu.ecnu.touchstone.datatype;

public class TSBool implements TSDataTypeInfo {
	
	private static final long serialVersionUID = 1L;
	
	private float nullRatio;
	private float trueRatio;

	public TSBool() {
		super();
		nullRatio = 0;
		trueRatio = 0.5f;
	}

	public TSBool(float nullRatio) {
		super();
		this.nullRatio = nullRatio;
		trueRatio = 0.5f;
	}

	public TSBool(float nullRatio, float trueRatio) {
		super();
		this.nullRatio = nullRatio;
		this.trueRatio = trueRatio;
	}
	
	public TSBool(TSBool tsBool) {
		super();
		this.nullRatio = tsBool.nullRatio;
		this.trueRatio = tsBool.trueRatio;
	}
	
	@Override
	public Boolean geneData() {
		if (Math.random() < nullRatio) {
			return null;
		}
		if (Math.random() < trueRatio) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "TSBool [nullRatio=" + nullRatio + ", trueRatio=" + trueRatio + "]";
	}

	@Override
	public double getMinValue() {
		return 0;
	}

	@Override
	public double getMaxValue() {
		return 1;
	}
}
