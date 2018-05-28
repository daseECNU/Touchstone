package edu.ecnu.touchstone.schema;

import java.io.Serializable;

import edu.ecnu.touchstone.datatype.TSBool;
import edu.ecnu.touchstone.datatype.TSDataTypeInfo;
import edu.ecnu.touchstone.datatype.TSDate;
import edu.ecnu.touchstone.datatype.TSDateTime;
import edu.ecnu.touchstone.datatype.TSDecimal;
import edu.ecnu.touchstone.datatype.TSInteger;
import edu.ecnu.touchstone.datatype.TSReal;
import edu.ecnu.touchstone.datatype.TSVarchar;

public class Attribute implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String attrName = null;
	private String dataType = null;
	
	// information of basic data characteristics
	private TSDataTypeInfo dataTypeInfo = null;

	public Attribute(String attrName, String dataType, TSDataTypeInfo dataTypeInfo) {
		super();
		this.attrName = attrName;
		this.dataType = dataType;
		this.dataTypeInfo = dataTypeInfo;
	}
	
	public Attribute(Attribute attribute) {
		super();
		this.attrName = attribute.attrName;
		this.dataType = attribute.dataType;
		switch (this.dataType) {
		case "integer":
			this.dataTypeInfo = new TSInteger((TSInteger)attribute.dataTypeInfo);
			break;
		case "real":
			this.dataTypeInfo = new TSReal((TSReal)attribute.dataTypeInfo);
			break;
		case "decimal":
			this.dataTypeInfo = new TSDecimal((TSDecimal)attribute.dataTypeInfo);
			break;
		case "date":
			this.dataTypeInfo = new TSDate((TSDate)attribute.dataTypeInfo);
			break;
		case "datetime":
			this.dataTypeInfo = new TSDateTime((TSDateTime)attribute.dataTypeInfo);
			break;
		case "varchar":
			this.dataTypeInfo = new TSVarchar((TSVarchar)attribute.dataTypeInfo);
			break;
		case "bool":
			this.dataTypeInfo = new TSBool((TSBool)attribute.dataTypeInfo);
			break;
		}
	}
	
	// automatically acquire the data characteristics -- DBStatisticsCollector
	public void setDataTypeInfo(TSDataTypeInfo dataTypeInfo) {
		this.dataTypeInfo = dataTypeInfo;
	}

	public String geneData() {
		return dataTypeInfo.geneData().toString();
	}

	public String getAttrName() {
		return attrName;
	}

	public String getDataType() {
		return dataType;
	}

	public TSDataTypeInfo getDataTypeInfo() {
		return dataTypeInfo;
	}

	@Override
	public String toString() {
		return "\n\tAttribute [attrName=" + attrName + ", dataType=" + dataType + ", dataTypeInfo=" + dataTypeInfo + "]";
	}
}
