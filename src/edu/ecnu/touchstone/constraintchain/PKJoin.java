package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;
import java.util.Arrays;

// pattern: [1, pk1#pk2 ..., num1, num2, ...]
// num1 is the identifier that can join, num2 is the identifier that can not join
// for every primary key, the identifier (num1 and num2) must be unique
// num1 and num2 can appear multiple pairs (a primary key may be joined with multiple associated foreign keys)
// the primary key can be multiple attributes (support mixed reference)
public class PKJoin implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String[] primakryKeys = null;
	
	// aligned in sequence
	private int[] canJoinNum = null;
	private int[] cantJoinNum = null;
	
	// to avoid the string manipulation in data generation
	private String pkStr = null;

	public PKJoin(String[] primakryKeys, int[] canJoinNum, int[] cantJoinNum) {
		super();
		this.primakryKeys = primakryKeys;
		this.canJoinNum = canJoinNum;
		this.cantJoinNum = cantJoinNum;
		pkStr = Arrays.toString(this.primakryKeys);
	}
	
	public PKJoin(PKJoin pkJoin) {
		super();
		this.primakryKeys = Arrays.copyOf(pkJoin.primakryKeys, pkJoin.primakryKeys.length);
		this.canJoinNum = Arrays.copyOf(pkJoin.canJoinNum, pkJoin.canJoinNum.length);
		this.cantJoinNum = Arrays.copyOf(pkJoin.cantJoinNum, pkJoin.cantJoinNum.length);
		this.pkStr = pkJoin.pkStr;
	}

	public String[] getPrimakryKeys() {
		return primakryKeys;
	}

	public int[] getCanJoinNum() {
		return canJoinNum;
	}

	public int[] getCantJoinNum() {
		return cantJoinNum;
	}

	public String getPkStr() {
		return pkStr;
	}

	@Override
	public String toString() {
		return "\n\tPKJoin [primakryKeys=" + Arrays.toString(primakryKeys) + ", canJoinNum=" + Arrays.toString(canJoinNum)
				+ ", cantJoinNum=" + Arrays.toString(cantJoinNum) + ", pkStr=" + pkStr + "]";
	}
}