package edu.ecnu.touchstone.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TSInteger implements TSDataTypeInfo {

	private static final long serialVersionUID = 1L;
	
	private float nullRatio;
	private long cardinality;
	private long min;
	private long max;

	// reduce the amount of calculation: quotiety = (max - min) / cardinality
	private float quotiety;

	// map: index -> probability
	private Map<Long, Double> indexProbabilityMap = null;
	private List<AdjustedIndex> adjustedIndexes = null;
	private double avgProbability;

	// piecewise function representation of this attribute
	private String piecewiseFunction = null;

	public TSInteger() {
		super();
		nullRatio = 0;
		cardinality = 1000000;
		min = -100000000;
		max = 100000000;
		init();
	}

	public TSInteger(float nullRatio, long cardinality, long min, long max) {
		super();
		this.nullRatio = nullRatio;
		this.cardinality = cardinality;
		this.min = min;
		this.max = max;
		init();
	}
	
	public TSInteger(TSInteger tsInteger) {
		super();
		this.nullRatio = tsInteger.nullRatio;
		this.cardinality = tsInteger.cardinality;
		this.min = tsInteger.min;
		this.max = tsInteger.max;
		this.quotiety = tsInteger.quotiety;
		this.indexProbabilityMap = new HashMap<Long, Double>();
		this.indexProbabilityMap.putAll(tsInteger.indexProbabilityMap);
		this.adjustedIndexes = new ArrayList<AdjustedIndex>();
		for (int i = 0; i < tsInteger.adjustedIndexes.size(); i++) {
			this.adjustedIndexes.add(new AdjustedIndex(tsInteger.adjustedIndexes.get(i)));
		}
		this.avgProbability = tsInteger.avgProbability;
		this.piecewiseFunction = tsInteger.piecewiseFunction;
	}

	private void init() {
		quotiety = (float)(max - min) / cardinality;
		indexProbabilityMap = new HashMap<Long, Double>();
		adjustedIndexes = new ArrayList<AdjustedIndex>();
		avgProbability = 1d / cardinality;
	}

	@Override
	public Long geneData() {
		if (Math.random() < nullRatio) {
			return null;
		}
		long randomIndex = getCorrespondingIndex(Math.random());
		return (long)(quotiety * randomIndex + min);
	}

	public long getCorrespondingIndex(double randomValue) {
		if (adjustedIndexes.size() == 0) {
			return (long)(randomValue * cardinality);
		}
		
		long randomIndex = -1;
		for (int i = 0; i < adjustedIndexes.size(); i++) {
			if (randomValue < adjustedIndexes.get(i).startCumulativeProbability) {
				if (i == 0) {
					randomIndex = (long)(randomValue / avgProbability);
				} else {
					long frontIndex = adjustedIndexes.get(i - 1).index;
					long increment = (long)((randomValue - adjustedIndexes.get(i - 1).endCumulativeProbability) 
							/ avgProbability) + 1;
					randomIndex = frontIndex + increment;
				}
				break;
			} else if (randomValue >= adjustedIndexes.get(i).startCumulativeProbability
					&& randomValue < adjustedIndexes.get(i).endCumulativeProbability) {
				randomIndex = adjustedIndexes.get(i).index;
				break;
			}
		}
		
		if (randomIndex != -1) {
			return randomIndex;
		} else {
			int i = adjustedIndexes.size() - 1;
			long frontIndex = adjustedIndexes.get(i).index;
			long increment = (int)((randomValue - adjustedIndexes.get(i).endCumulativeProbability) 
					/ avgProbability) + 1;
			randomIndex = frontIndex + increment;
			return randomIndex;
		}
	}

	// overall control needs to be done in call place
	public long adjustValueProbability(double probability) {
		long randomIndex = (long)(Math.random() * cardinality);
		while (indexProbabilityMap.containsKey(randomIndex)) {
			randomIndex = (long)(Math.random() * cardinality);
		}
		indexProbabilityMap.put(randomIndex, probability);
		return (long)(quotiety * randomIndex + min);
	}

	public void reconstituteGeneFunction() {
		Iterator<Entry<Long, Double>> iterator = indexProbabilityMap.entrySet().iterator();
		double sum = 0;
		while (iterator.hasNext()) {
			Entry<Long, Double> entry = iterator.next();
			sum += entry.getValue();
			adjustedIndexes.add(new AdjustedIndex(entry.getKey()));
		}
		Collections.sort(adjustedIndexes);
		avgProbability = (1 - sum) / (cardinality - adjustedIndexes.size());
		long frontIndex = 0;
		double frontEndCumulativeProbability = 0;
		for (int i = 0; i < adjustedIndexes.size(); i++) {
			AdjustedIndex adjustedIndex = adjustedIndexes.get(i);
			adjustedIndex.startCumulativeProbability = (adjustedIndex.index - frontIndex) 
					* avgProbability + frontEndCumulativeProbability;
			adjustedIndex.endCumulativeProbability = adjustedIndex.startCumulativeProbability + 
					indexProbabilityMap.get(adjustedIndex.index);
			frontIndex = adjustedIndex.index + 1;
			frontEndCumulativeProbability = adjustedIndex.endCumulativeProbability;
		}
	}

	// clear all the information of adjusted indexes
	public void clear() {
		indexProbabilityMap.clear();
		adjustedIndexes.clear();
		avgProbability = 1d / cardinality;
		piecewiseFunction = null;
	}

	public String getPiecewiseFunction(String variable) {
		if (piecewiseFunction != null) {
			return piecewiseFunction;
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("Piecewise[{");
		long frontValue = min;
		double frontProbability = 0;
		for (int i = 0; i < adjustedIndexes.size(); i++) {
			AdjustedIndex adjustedindex = adjustedIndexes.get(i);
			long value = (long)(quotiety * adjustedindex.index + min);
			double start = adjustedindex.startCumulativeProbability;
			double end = adjustedindex.endCumulativeProbability;
			double k = (value - frontValue) / (start - frontProbability);
			sb.append("{" + k + " * (" + variable + " - " + frontProbability + ") + " + frontValue + 
					", " + frontProbability + " <= " + variable + " < " + start + "}, ");
			sb.append("{" + value + ", " + start + " <= " + variable + " < " + end + "}, ");
			frontValue = value;
			frontProbability = end;
		}
		double k = (max - frontValue) / (1 - frontProbability);
		sb.append("{" + k + " * (" + variable + " - " + frontProbability + ") + " + 
				frontValue + ", " + frontProbability + " <= " + variable + " < 1}}]");
		piecewiseFunction = sb.toString();
		return piecewiseFunction;
	}
	
	public boolean isAdjusted() {
		if (adjustedIndexes.size() == 0) {
			return false;
		} else {
			return true;
		}
	}
	
	public Long getGeneData(long randomIndex) {
		return (long)(quotiety * randomIndex + min);
	}
	
	public Map<Long, Double> getIndexProbabilityMap() {
		return indexProbabilityMap;
	}
	
	public double getMinValue() {
		return min;
	}

	public double getMaxValue() {
		return max;
	}

	@Override
	public String toString() {
		return "TSInteger [nullRatio=" + nullRatio + ", cardinality=" + cardinality + ", min=" + min + ", max=" + max
				+ ", quotiety=" + quotiety + ", indexProbabilityMap=" + indexProbabilityMap + ", adjustedIndexes="
				+ adjustedIndexes + ", avgProbability=" + avgProbability + ", piecewiseFunction=" + piecewiseFunction
				+ "]";
	}
}

class AdjustedIndex implements Comparable<AdjustedIndex>, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	double startCumulativeProbability;
	double endCumulativeProbability;
	long index;

	public AdjustedIndex(long index) {
		super();
		this.index = index;
	}
	
	public AdjustedIndex(AdjustedIndex adjustedIndex) {
		super();
		this.startCumulativeProbability = adjustedIndex.startCumulativeProbability;
		this.endCumulativeProbability = adjustedIndex.endCumulativeProbability;
		this.index = adjustedIndex.index;
	}

	@Override
	public String toString() {
		return "AdjustedIndex [startCumulativeProbability=" + startCumulativeProbability + ", endCumulativeProbability="
				+ endCumulativeProbability + ", index=" + index + "]";
	}

	@Override
	public int compareTo(AdjustedIndex other) {
		if (this.index < other.index) {
			return -1;
		} else {
			return 1;
		}
	}
}
