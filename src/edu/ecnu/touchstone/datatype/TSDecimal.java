package edu.ecnu.touchstone.datatype;

public class TSDecimal extends TSReal {
	
	private static final long serialVersionUID = 1L;

	public TSDecimal() {
		super();
	}
	
	public TSDecimal(float nullRatio, double min, double max) {
		super(nullRatio, min, max);
	}
	
	public TSDecimal(TSDecimal tsDecimal) {
		super();
		this.nullRatio = tsDecimal.nullRatio;
		this.min = tsDecimal.min;
		this.max = tsDecimal.max;
	}
	
	@Override
	public String toString() {
		return "TSDecimal [nullRatio=" + nullRatio + ", min=" + min + ", max=" + max + "]";
	}
}
