package edu.ecnu.touchstone.datatype;

import java.io.Serializable;

public interface TSDataTypeInfo extends Serializable {
	
	public Object geneData();
	
	public double getMinValue();
	
	public double getMaxValue();
}
