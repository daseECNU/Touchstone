package edu.ecnu.touchstone.queryinstantiation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.ecnu.touchstone.schema.Attribute;

public class ComputingTask {

	private int id;
	private String expression = null;
	// only can be >, >=, <, <= (bet is expressed by >= and <)
	private String operator = null;
	// 'attrNames' and 'attributes' are aligned in sequence
	private List<String> attrNames = null;
	private List<Attribute> attributes = null;
	private float probability;
	private float inputDataSize;
	private boolean isBet;

	// support non-equi join
	private List<String> childrensConstraints = null;
	private List<Integer> children = null;

	// range filters of cardinality constraints
	public ComputingTask(int id, String expression, String operator, float probability, 
			Map<String, Attribute> attributeMap, String tableName, float inputDataSize, boolean isBet) {
		this.id = id;
		this.operator = operator;
		this.probability = probability;
		this.inputDataSize = inputDataSize;
		this.isBet = isBet;

		// as the variables of 'expression'
		attrNames = new ArrayList<String>();
		attributes = new ArrayList<Attribute>();
		String[] arr = expression.split("[\\+\\-\\*/\\^\\(\\)]");
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < arr.length; i++) {
			if (!arr[i].matches("[\\d\\.]*")) {
				// an attribute may appear multiple times in an expression
				if (set.contains(arr[i])) {
					continue;
				} else {
					// in parsii, characters '.' and '_' need to be removed from the name of 
					// attributes because they are the protected character
					set.add(arr[i]);
					String tmp = arr[i].replaceAll("[\\._]", "");
					attrNames.add(tmp);
					expression = expression.replaceAll(arr[i], tmp);
					attributes.add(attributeMap.get(tableName + "." + arr[i]));
				}
			}
		}
		this.expression = expression;
	}

	// support non-equi join workload
	public ComputingTask(int id, String expression, String operator, float probability, List<Integer> children, 
			Map<String, Attribute> attributeMap, Map<Integer, ComputingTask> taskMap, float inputDataSize, boolean isBet) {
		this.id = id;
		this.operator = operator;
		this.probability = probability;
		this.children = children;
		this.inputDataSize = inputDataSize;
		this.isBet = isBet;
		childrensConstraints = new ArrayList<String>();

		attrNames = new ArrayList<String>();
		attributes = new ArrayList<Attribute>();
		String[] arr = expression.split("[\\+\\-\\*/\\^\\(\\)]");
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < arr.length; i++) {
			if (!arr[i].matches("[\\d\\.]*")) {
				if (set.contains(arr[i])) {
					continue;
				} else {
					set.add(arr[i]);
					String tmp = arr[i].replaceAll("[\\._]", "");
					attrNames.add(tmp);
					expression = expression.replaceAll(arr[i], tmp);
					attributes.add(attributeMap.get(arr[i]));
				}
			}
		}
		this.expression = expression;

		// get the information of all children's attributes
		set = set.stream().map(x -> x.replaceAll("[\\._]", "")).collect(Collectors.toSet());
		for (int i = 0; i < children.size(); i++) {
			List<String> childsAttrNames = taskMap.get(children.get(i)).getAttrNames();
			List<Attribute> childsAttributes = taskMap.get(children.get(i)).getAttributes();
			for (int j = 0; j < childsAttrNames.size(); j++) {
				if (!set.contains(childsAttrNames.get(j))) {
					attrNames.add(childsAttrNames.get(j));
					attributes.add(childsAttributes.get(j));
					set.add(childsAttrNames.get(j));
				}
			}
		}
	}

	public int getId() {
		return id;
	}

	public String getExpression() {
		return expression;
	}

	public String getOperator() {
		return operator;
	}

	public List<String> getAttrNames() {
		return attrNames;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public float getProbability() {
		return probability;
	}

	public float getInputDataSize() {
		return inputDataSize;
	}

	public boolean isBet() {
		return isBet;
	}

	public List<String> getChildrensConstraints() {
		return childrensConstraints;
	}

	public List<Integer> getChildren() {
		return children;
	}

	@Override
	public String toString() {
		return "\n\tComputingTask [id=" + id + ", expression=" + expression + ", operator=" + operator + ", attrNames="
				+ attrNames + ", attributes=" + attributes + ", probability=" + probability + ", inputDataSize="
				+ inputDataSize + ", childrensConstraints=" + childrensConstraints + ", children=" + children + "]";
	}
}
