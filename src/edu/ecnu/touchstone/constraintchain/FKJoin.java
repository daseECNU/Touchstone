package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;
import java.util.Arrays;

// FKJoin node of a constraint chain
// pattern: [2, fk1#fk2 ..., probability, pk1#pk2 ..., num1, num2]
// pk must have a table name prefix
public class FKJoin implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String[] foreignKeys = null;
	private float probability;
	private String[] primakryKeys = null;
	private int canJoinNum;
	private int cantJoinNum;

	// to avoid the string manipulation in data generation
	private String fkStr = null;
	private String rpkStr = null;
	
	// to solve the 'special situation'
	private transient FKJoinAdjustment fkJoinAdjustment = null;
	
	// it's needed for generating 'fkJoinAdjustment' (its attribute 'probability')
	private transient float accumulativeProbability;
	
	public FKJoin(String[] foreignKeys, float probability, String[] primakryKeys, int canJoinNum, int cantJoinNum) {
		super();
		this.foreignKeys = foreignKeys;
		this.probability = probability;
		this.primakryKeys = primakryKeys;
		this.canJoinNum = canJoinNum;
		this.cantJoinNum = cantJoinNum;
		fkStr = Arrays.toString(this.foreignKeys);
		rpkStr = Arrays.toString(this.primakryKeys);
	}
	
	public FKJoin(FKJoin fkJoin) {
		super();
		this.foreignKeys = Arrays.copyOf(fkJoin.foreignKeys, fkJoin.foreignKeys.length);
		this.probability = fkJoin.probability;
		this.primakryKeys = Arrays.copyOf(fkJoin.primakryKeys, fkJoin.primakryKeys.length);
		this.canJoinNum = fkJoin.canJoinNum;
		this.cantJoinNum = fkJoin.cantJoinNum;
		this.fkStr = fkJoin.fkStr;
		this.rpkStr = fkJoin.rpkStr;
	}

	public void setFkJoinAdjustment(FKJoinAdjustment fkJoinAdjustment) {
		this.fkJoinAdjustment = fkJoinAdjustment;
	}
	
	public void setAccumulativeProbability(float accumulativeProbability) {
		this.accumulativeProbability = accumulativeProbability;
	}

	public String[] getForeignKeys() {
		return foreignKeys;
	}

	public float getProbability() {
		return probability;
	}

	public String[] getPrimakryKeys() {
		return primakryKeys;
	}

	public int getCanJoinNum() {
		return canJoinNum;
	}

	public int getCantJoinNum() {
		return cantJoinNum;
	}

	public String getFkStr() {
		return fkStr;
	}

	public String getRpkStr() {
		return rpkStr;
	}

	public FKJoinAdjustment getFkJoinAdjustment() {
		return fkJoinAdjustment;
	}
	
	public float getAccumulativeProbability() {
		return accumulativeProbability;
	}

	public boolean canJoin() {
		return fkJoinAdjustment.canJoin();
	}

	@Override
	public String toString() {
		return "\n\tFKJoin [foreignKeys=" + Arrays.toString(foreignKeys) + ", probability=" + probability 
				+ ", primakryKeys=" + Arrays.toString(primakryKeys) + ", canJoinNum=" + canJoinNum 
				+ ", cantJoinNum=" + cantJoinNum + ", fkStr=" + fkStr + ", rpkStr=" + rpkStr  + "]";
	}
}
