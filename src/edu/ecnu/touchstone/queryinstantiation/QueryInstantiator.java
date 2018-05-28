package edu.ecnu.touchstone.queryinstantiation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.constraintchain.CCNode;
import edu.ecnu.touchstone.constraintchain.ConstraintChain;
import edu.ecnu.touchstone.constraintchain.ConstraintChainsReader;
import edu.ecnu.touchstone.constraintchain.FKJoin;
import edu.ecnu.touchstone.constraintchain.Filter;
import edu.ecnu.touchstone.constraintchain.FilterOperation;
import edu.ecnu.touchstone.datatype.TSInteger;
import edu.ecnu.touchstone.datatype.TSVarchar;
import edu.ecnu.touchstone.nonequijoin.NonEquiJoinConstraint;
import edu.ecnu.touchstone.nonequijoin.NonEquiJoinConstraintsReader;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.Attribute;
import edu.ecnu.touchstone.schema.SchemaReader;
import edu.ecnu.touchstone.schema.Table;

public class QueryInstantiator {

	// input data
	private List<Table> tables = null;
	private List<ConstraintChain> constraintChains = null;
	private List<NonEquiJoinConstraint> nonEquiJoinConstraints = null;
	
	// instantiated parameters
	private List<Parameter> parameters = null;
	
	private Logger logger = null;
	private int maxIterations;
	private double requiredGlobalRelativeError;
	private ComputingThreadPool computingThreadPool = null;

	// map: tableName.attrName -> attribute
	private Map<String, Attribute> attributeMap = null;
	
	// map: tableName -> table
	private Map<String, Table> tableMap = null;
	
	// map: tableName.attrName -> equality filter operations
	private Map<String, ArrayList<FilterOperation>> attrEquaFilterOperMap = null;

	public QueryInstantiator(List<Table> tables, List<ConstraintChain> constraintChains, 
			List<NonEquiJoinConstraint> nonEquiJoinConstraints, int maxIterations, 
			double requiredGlobalRelativeError, ComputingThreadPool computingThreadPool) {
		this.tables = tables;
		this.constraintChains = constraintChains;
		this.nonEquiJoinConstraints = nonEquiJoinConstraints;
		this.maxIterations = maxIterations;
		this.requiredGlobalRelativeError = requiredGlobalRelativeError;
		this.computingThreadPool = computingThreadPool;
		init();
	}

	private void init() {
		logger = Logger.getLogger(Touchstone.class);
		parameters = new ArrayList<Parameter>();
		if (constraintChains == null) {
			constraintChains = Collections.emptyList();
		}
		if (nonEquiJoinConstraints == null) {
			nonEquiJoinConstraints = Collections.emptyList();
		}

		attributeMap = new HashMap<String, Attribute>();
		tableMap = new HashMap<String, Table>();
		for (int i = 0; i < tables.size(); i++) {
			String tableName = tables.get(i).getTableName();
			tableMap.put(tableName, tables.get(i));
			List<Attribute> attributes = tables.get(i).getAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				String attrName = attributes.get(j).getAttrName();
				attributeMap.put(tableName + "." + attrName, attributes.get(j));
			}
		}

		attrEquaFilterOperMap = new HashMap<String, ArrayList<FilterOperation>>();
		for (int i = 0; i < constraintChains.size(); i++) {
			String tableName = constraintChains.get(i).getTableName();
			List<CCNode> nodes = constraintChains.get(i).getNodes();
			for (int j = 0; j < nodes.size(); j++) {
				// is not a filter
				if (nodes.get(j).getType() != 0) {
					continue;
				}
				Filter filter = (Filter)nodes.get(j).getNode();
				FilterOperation[] filterOperations = filter.getFilterOperations();
				for (int k = 0; k < filterOperations.length; k++) {
					String operator = filterOperations[k].getOperator();
					// is not a equality filter operation
					if (!operator.equals("=") && !operator.equals("like") && !operator.matches("in\\([0-9]+\\)")) {
						continue;
					}
					String attrName = filterOperations[k].getExpression();
					String tmp = tableName + "." + attrName;
					if (!attrEquaFilterOperMap.containsKey(tmp)) {
						attrEquaFilterOperMap.put(tmp, new ArrayList<FilterOperation>());
					}
					attrEquaFilterOperMap.get(tmp).add(filterOperations[k]);
				}
			}
		}

		for (int i = 0; i < nonEquiJoinConstraints.size(); i++) {
			NonEquiJoinConstraint nonEquiJoinConstraint = nonEquiJoinConstraints.get(i);
			String operator = nonEquiJoinConstraint.getOperator();
			// is not a equality filter operation
			if (!operator.equals("=") && !operator.equals("like") && !operator.matches("in\\([0-9]+\\)")) {
				continue;
			}
			int id = nonEquiJoinConstraint.getId();
			String tmp = nonEquiJoinConstraint.getExpression();
			String expression = tmp.split(".")[1];
			// for non-equi join workload, assuming that the filter operation is at the bottom of the query tree
			float probability = nonEquiJoinConstraint.getProbability();
			/* // when the filter operation is not at the bottom of the query tree
			if (nonEquiJoinConstraint.children.size() != 0) {
				// must have only one child
				int childId = nonEquiJoinConstraint.children.get(0);
				for (int j = 0; j < nonEquiJoinConstraints.size(); j++) {
					if (nonEquiJoinConstraints.get(j).id == childId) {
						probability /= nonEquiJoinConstraints.get(j).probability;
						break;
					}
				}
			}*/
			if (!attrEquaFilterOperMap.containsKey(tmp)) {
				attrEquaFilterOperMap.put(tmp, new ArrayList<FilterOperation>());
			}
			attrEquaFilterOperMap.get(tmp).add(new FilterOperation(id, expression, operator, probability));
		}
	}

	public void iterate() {
		List<Parameter> bestParameters = new ArrayList<Parameter>();
		double bestGlobalRelativeError = Double.MAX_VALUE;
		long startTime = System.currentTimeMillis();
		
		for(int i = 0; i < maxIterations; i++) {
			clear();
			adjustValueProbability();
			instantiateParameters();
			computingThreadPool.waitFinished();
			parameters.addAll(computingThreadPool.getParameterMap().values());
			computingThreadPool.clearParameterMap();
			Collections.sort(parameters);

			long cardinalitySum = 0;
			long deviationSum = 0;
			for (int j = 0; j < parameters.size(); j++) {
				cardinalitySum += parameters.get(j).getCardinality();
				deviationSum += parameters.get(j).getDeviation();
			}
			double globalRelativeError = (double)deviationSum / cardinalitySum;

			logger.debug("\nquery instantiation iterations: " + i + "\ninstantiated parameters: " + parameters + 
					"\nglobal relative error: " + globalRelativeError);

			if (globalRelativeError < bestGlobalRelativeError) {
				bestParameters.clear();
				bestParameters.addAll(parameters);
				bestGlobalRelativeError = globalRelativeError;
			}
			if (bestGlobalRelativeError < requiredGlobalRelativeError) {
				break;
			}
		}
		
		long endTime = System.currentTimeMillis();
		logger.info("\n\tTime of query instantiation: " + (endTime - startTime) + "ms");
		logger.debug("\nFinal instantiated parameters: " + bestParameters + 
				"\nFinal global relative error: " + bestGlobalRelativeError);
	}

	private void clear() {
		parameters.clear();
		// initialize the generative function of attributes
		for (int i = 0; i < tables.size(); i++) {
			List<Attribute> attributes = tables.get(i).getAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				if (attributes.get(j).getDataType().equals("integer")) {
					((TSInteger)attributes.get(j).getDataTypeInfo()).clear();
				} else if (attributes.get(j).getDataType().equals("varchar")) {
					((TSVarchar)attributes.get(j).getDataTypeInfo()).clear();
				}
			}
		}
	}

	private void adjustValueProbability() {
		Iterator<Entry<String, ArrayList<FilterOperation>>> iterator = attrEquaFilterOperMap.entrySet().iterator();
		// map: probability -> instantiated parameter value
		Map<Float, String> probParaValueMap = new HashMap<Float, String>();
		while (iterator.hasNext()) {
			Entry<String, ArrayList<FilterOperation>> entry = iterator.next();
			Attribute attribute = attributeMap.get(entry.getKey());
			Table table = tableMap.get(entry.getKey().split("\\.")[0]);
			ArrayList<FilterOperation> equaFilterOperations = entry.getValue();
			Collections.shuffle(equaFilterOperations);

			float sum = 0;
			for (int i = 0; i < equaFilterOperations.size(); i++) {
				sum += equaFilterOperations.get(i).getProbability();
			}
			// if overflow > 0, some instantiated parameter values (with same expected probability) must be the same
			float overflow = sum - 1;
			probParaValueMap.clear();

			for (int i = 0; i < equaFilterOperations.size(); i++) {
				String dataType = attribute.getDataType();
				String operator = equaFilterOperations.get(i).getOperator();
				float probability = equaFilterOperations.get(i).getProbability();

				int id = equaFilterOperations.get(i).getId();
				List<String> values = new ArrayList<String>();
				long cardinality = (long)(table.getTableSize() * probability);

				if (operator.equals("=")) {
					if (overflow > 0 && probParaValueMap.containsKey(probability)) {
						values.add(probParaValueMap.get(probability));
						overflow -= probability;
					} else {
						if (dataType.equals("integer")) {
							TSInteger dataTypeInfo = (TSInteger)attribute.getDataTypeInfo();
							Long paraValue = dataTypeInfo.adjustValueProbability(probability);
							probParaValueMap.put(probability, paraValue.toString());
							values.add(paraValue.toString());
						} else if (dataType.equals("varchar")) {
							TSVarchar dataTypeInfo = (TSVarchar)attribute.getDataTypeInfo();
							String paraValue = dataTypeInfo.addEqualCandidate(probability);
							probParaValueMap.put(probability, paraValue);
							values.add(paraValue);
						}
					}
				} else if (operator.matches("in\\([0-9]+\\)")) {
					// the parameter values in an 'in' operator can not be the same
					int index1 = operator.indexOf('(');
					int index2 = operator.indexOf(')');
					int size = Integer.parseInt(operator.substring(index1 + 1, index2));
					float subProbability = probability / size;
					int j;
					if (overflow > 0 && probParaValueMap.containsKey(subProbability)) {
						values.add(probParaValueMap.get(subProbability));
						overflow -= subProbability;
						j = 1;
					} else {
						j = 0;
						if (dataType.equals("integer")) {
							TSInteger dataTypeInfo = (TSInteger)attribute.getDataTypeInfo();
							for (; j < size; j++) {
								Long paraValue = dataTypeInfo.adjustValueProbability(subProbability);
								probParaValueMap.put(subProbability, paraValue.toString());
								values.add(paraValue.toString());
							}
						} else if (dataType.equals("varchar")) {
							TSVarchar dataTypeInfo = (TSVarchar)attribute.getDataTypeInfo();
							for (; j < size; j++) {
								String paraValue = dataTypeInfo.addEqualCandidate(subProbability);
								probParaValueMap.put(subProbability, paraValue);
								values.add(paraValue);
							}
						}
					}
				} else if (operator.equals("like")) {
					if (dataType.equals("varchar")) {
						TSVarchar dataTypeInfo = (TSVarchar)attribute.getDataTypeInfo();
						String paraValue = dataTypeInfo.addLikeCandidate(probability);
						values.add(paraValue);
					}
				}

				if (values.size() == 0) {
					logger.error("\n\tCan not handle the basic filter operation: " + equaFilterOperations.get(i));
					System.exit(0);
				} else {
					parameters.add(new Parameter(id, values, cardinality, 0));
				}
			}
		}

		// reconstitute the generative function of integer typed attributes
		for (int i = 0; i < tables.size(); i++) {
			List<Attribute> attributes = tables.get(i).getAttributes();
			for (int j = 0; j < attributes.size(); j++) {
				if (attributes.get(j).getDataType().equals("integer")) {
					((TSInteger)attributes.get(j).getDataTypeInfo()).reconstituteGeneFunction();
				}
			}
		}
		
		Collections.sort(parameters);
		logger.debug("\nThe parameters after handling the equality filter operations: " + parameters);
		logger.debug("\nThe schema after handling the equality filter operations: " + tables);
	}

	private void instantiateParameters() {
		for (int i = 0; i < constraintChains.size(); i++) {
			ConstraintChain chain = constraintChains.get(i);
			Table table = tableMap.get(chain.getTableName());
			List<CCNode> nodes = chain.getNodes();
			float inputDataSize = table.getTableSize();

			for (int j = 0; j < nodes.size(); j++) {
				int type = nodes.get(j).getType();
				if (type != 0) {
					// calculate the input data size of next node
					// note: assuming that 'PKJoin' node is the last node of a chain
					if (type == 2) {
						FKJoin fkJoin = (FKJoin)nodes.get(j).getNode();
						inputDataSize = inputDataSize * fkJoin.getProbability();
					}
					continue;
				}
				
				Filter filter = (Filter)nodes.get(j).getNode();
				FilterOperation[] filterOperations = filter.getFilterOperations();
				for (int k = 0; k < filterOperations.length; k++) {
					String operator = filterOperations[k].getOperator();
					if (operator.equals("=") || operator.equals("like") || operator.matches("in\\([0-9]+\\)")) {
						continue;
					}
					// the instantiated targets are: 'filter operations' && 'operator is one of >, >=, <, <=, bet'
					// support parallel calculation
					int id = filterOperations[k].getId();
					String expression = filterOperations[k].getExpression();
					float probability = filterOperations[k].getProbability();

					if (operator.equals("bet")) {
						float probability1 = (float)Math.random() * (1 - probability);
						float probability2 = probability1 + probability;
						computingThreadPool.addTask(new ComputingTask(id, expression, ">=", 1 - probability1, 
								attributeMap, table.getTableName(), inputDataSize, true));
						computingThreadPool.addTask(new ComputingTask(id, expression, "<", probability2, 
								attributeMap, table.getTableName(), inputDataSize, true));
					} else {
						computingThreadPool.addTask(new ComputingTask(id, expression, operator, probability, 
								attributeMap, table.getTableName(), inputDataSize, false));
					}
				} // for filterOperations
				inputDataSize = inputDataSize * filter.getProbability();
			} // for nodes
		}// for constraintChains

		Map<Integer, ComputingTask> taskMap = new HashMap<Integer, ComputingTask>();
		for (int i = 0; i < nonEquiJoinConstraints.size(); i++) {
			NonEquiJoinConstraint nonEquiJoinConstraint = nonEquiJoinConstraints.get(i);
			int id = nonEquiJoinConstraint.getId();
			String expression = nonEquiJoinConstraint.getExpression();
			String operator = nonEquiJoinConstraint.getOperator();
			float probability = nonEquiJoinConstraint.getProbability();
			float inputDataSize = nonEquiJoinConstraint.getInputDataSize();
			List<Integer> children = nonEquiJoinConstraint.getChildren();

			if (operator.equals("bet")) {
				float probability1 = (float)Math.random() * (1 - probability);
				float probability2 = probability1 + probability;
				ComputingTask task1 = new ComputingTask(id, expression, ">=", 1 - probability1, children, 
						attributeMap, taskMap, inputDataSize, true);
				ComputingTask task2 = new ComputingTask(id, expression, "<", probability2, children, 
						attributeMap, taskMap, inputDataSize, true);
				// only one is ok
				taskMap.put(id, task1);
				computingThreadPool.addTask(task1);
				computingThreadPool.addTask(task2);
			} else {
				ComputingTask task = new ComputingTask(id, expression, operator, probability, children, 
						attributeMap, taskMap, inputDataSize, false);
				taskMap.put(id, task);
				computingThreadPool.addTask(task);
			}
		} // for nonEquiJoinConstraints
	}

	public List<Parameter> getParameters() {
		return parameters;
	}

	// test
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		System.setProperty("com.wolfram.jlink.libdir", 
				"C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink");
		
//		SchemaReader schemaReader = new SchemaReader();
//		List<Table> tables = schemaReader.read(".//test//input//tpch_schema_sf_1.txt");
//		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
//		List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//tpch_cardinality_constraints_sf_1.txt");
//		ComputingThreadPool computingThreadPool = new ComputingThreadPool(2, 20, 0.00001);
//		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
//		queryInstantiator.iterate();
		
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(".//test//input//function_test_schema_0.txt");
		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
		List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//function_test_cardinality_constraints_0.txt");
		NonEquiJoinConstraintsReader nonEquiJoinConstraintsReader = new NonEquiJoinConstraintsReader();
		List<NonEquiJoinConstraint> nonEquiJoinConstraints = nonEquiJoinConstraintsReader.read(".//test//input//function_test_non_equi_join_0.txt");
//		ComputingThreadPool computingThreadPool = new ComputingThreadPool(1, 20, 0.00001);
		ComputingThreadPool computingThreadPool = new ComputingThreadPool(4, 20, 0.00001);
		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, nonEquiJoinConstraints, 20, 0.00001, computingThreadPool);
		queryInstantiator.iterate();
	}
}
