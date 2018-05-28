package edu.ecnu.touchstone.pretreatment;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.constraintchain.CCNode;
import edu.ecnu.touchstone.constraintchain.ConstraintChain;
import edu.ecnu.touchstone.constraintchain.ConstraintChainsReader;
import edu.ecnu.touchstone.constraintchain.Filter;
import edu.ecnu.touchstone.constraintchain.FilterOperation;
import edu.ecnu.touchstone.queryinstantiation.ComputingThreadPool;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.queryinstantiation.QueryInstantiator;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.Attribute;
import edu.ecnu.touchstone.schema.ForeignKey;
import edu.ecnu.touchstone.schema.SchemaReader;
import edu.ecnu.touchstone.schema.Table;

// main functions: 
// 1. get the partial order among tables
// 2. get the generation templates of tables
public class Preprocessor {

	private List<Table> tables = null;
	private List<ConstraintChain> constraintChains = null;
	private List<Parameter> parameters = null;
	private Logger logger = null;

	public Preprocessor(List<Table> tables, List<ConstraintChain> constraintChains, List<Parameter> parameters) {
		super();
		this.tables = tables;
		this.constraintChains = constraintChains;
		this.parameters = parameters;
		logger = Logger.getLogger(Touchstone.class);
	}

	// get the partial order among tables according to the foreign key constraints
	public List<String> getPartialOrder() {
		Set<String> allTables = new HashSet<String>();
		// the tables with a foreign key (or foreign keys)
		Set<String> nonMetaTables = new HashSet<String>();
		// map: table -> referenced tables (foreign key constraint)
		Map<String, ArrayList<String>> tableDependencyInfo = new HashMap<String, ArrayList<String>>();
		for (int i = 0; i < tables.size(); i++) {
			Table table = tables.get(i);
			allTables.add(table.getTableName());
			if (table.getForeignKeys().size() != 0) {
				nonMetaTables.add(table.getTableName());
				List<ForeignKey> foreignKeys = table.getForeignKeys();
				ArrayList<String> referencedTables = new ArrayList<String>();
				for (int j = 0; j < foreignKeys.size(); j++) {
					referencedTables.add(foreignKeys.get(j).getReferencedKey().split("\\.")[0]);
				}
				tableDependencyInfo.put(table.getTableName(), referencedTables);
			}
		}

		// the remaining tables are metadata tables
		allTables.removeAll(nonMetaTables);
		Set<String> partialOrder = new LinkedHashSet<String>();
		partialOrder.addAll(allTables);
		Iterator<Entry<String, ArrayList<String>>> iterator = tableDependencyInfo.entrySet().iterator();
		while (true) {
			while (iterator.hasNext()) {
				Entry<String, ArrayList<String>> entry = iterator.next();
				if (partialOrder.containsAll(entry.getValue())) {
					partialOrder.add(entry.getKey());
				}
			}
			if (partialOrder.size() == tables.size()) {
				break;
			}
			iterator = tableDependencyInfo.entrySet().iterator();
		}
		
		logger.debug("\nThe partial order of tables: \n\t" + partialOrder);
		return partialOrder.stream().collect(Collectors.toList());
	}

	// get the generation templates of all tables
	public Map<String, TableGeneTemplate> getTableGeneTemplates(int shuffleMaxNum, int pkvsMaxSize) {
		Map<Integer, Parameter> parameterMap = new HashMap<Integer, Parameter>();
		for (int j = 0; j < parameters.size(); j++) {
			parameterMap.put(parameters.get(j).getId(), parameters.get(j));
		}
		
		Map<String, TableGeneTemplate> tableGeneTemplateMap = new HashMap<String, TableGeneTemplate>();
		for (int i = 0; i < tables.size(); i++) {
			Table table = tables.get(i);
			String tableName = table.getTableName();
			long tableSize = table.getTableSize();
			String pkStr = table.getPrimaryKey().toString();
			List<Key> keys = new ArrayList<Key>();
			List<Attribute> attributes = table.getAttributes();
			List<ConstraintChain> tableConstraintChains = new ArrayList<ConstraintChain>();
			List<String> referencedKeys = new ArrayList<String>();
			Map<String, String> referKeyForeKeyMap = new HashMap<String, String>();
			Map<Integer, Parameter> localParameterMap = new HashMap<Integer, Parameter>();
			Map<String, Attribute> attributeMap = new HashMap<String, Attribute>();

			// keys
			List<String> primaryKey = table.getPrimaryKey();
			List<ForeignKey> foreignKeys = table.getForeignKeys();
			loop : for (int j = 0; j < primaryKey.size(); j++) {
				for (int k = 0; k < foreignKeys.size(); k++) {
					if (foreignKeys.get(k).getAttrName().equals(primaryKey.get(j).split("\\.")[1])) {
						continue loop;
					}
				}
				keys.add(new Key(primaryKey.get(j), 0));
			}
			// add an attribute to ensure the uniqueness of the primary key
			if (keys.size() == 0) {
				keys.add(new Key("unique_number", 0));
			}
			for (int j = 0; j < foreignKeys.size(); j++) {
				keys.add(new Key(tableName + "." + foreignKeys.get(j).getAttrName(), 1));
			}

			// tableConstraintChains
			for (int j = 0; j < constraintChains.size(); j++) {
				if (constraintChains.get(j).getTableName().equals(tableName)) {
					tableConstraintChains.add(constraintChains.get(j));
				}
			}

			// referencedKeys (support mixed reference)
			foreignKeys.sort((x, y) -> x.getReferencedKey().compareTo(y.getReferencedKey()));
			for (int index = 0, j = 0; j < foreignKeys.size(); j++) {
				if ((j < foreignKeys.size() - 1)) {
					if (foreignKeys.get(j).getReferencedKey().split("\\.")[0].equals(
							foreignKeys.get(j + 1).getReferencedKey().split("\\.")[0])) {
						continue;
					}
				}
				String fksStr = "[";
				for (int k = index; k <= j; k++) {
					fksStr = fksStr + foreignKeys.get(k).getReferencedKey();
					if (k != j) {
						fksStr = fksStr + ", ";
					}
				}
				fksStr = fksStr + "]";
				referencedKeys.add(fksStr);
				index = j + 1;
			}

			// referKeyForeKeyMap
			for (int j = 0; j < foreignKeys.size(); j++) {
				referKeyForeKeyMap.put(foreignKeys.get(j).getReferencedKey(), 
						tableName + "." + foreignKeys.get(j).getAttrName());
			}
			
			// localParameterMap
			for (int j = 0; j < tableConstraintChains.size(); j++) {
				List<CCNode> nodes = tableConstraintChains.get(j).getNodes();
				for (int k = 0; k < nodes.size(); k++) {
					if (nodes.get(k).getType() == 0) {
						Filter filter = (Filter)nodes.get(k).getNode();
						FilterOperation[] operations = filter.getFilterOperations();
						for (int l = 0; l < operations.length; l++) {
							localParameterMap.put(operations[l].getId(), parameterMap.get(operations[l].getId()));
						}
					}
				}
			}
			
			// attributeMap
			for (int j = 0; j < attributes.size(); j++) {
				attributeMap.put(attributes.get(j).getAttrName(), attributes.get(j));
			}

			TableGeneTemplate tableGeneTemplate = new TableGeneTemplate(tableName, tableSize, pkStr, 
					keys, attributes, tableConstraintChains, referencedKeys, referKeyForeKeyMap, 
					localParameterMap, attributeMap, shuffleMaxNum, pkvsMaxSize);
			tableGeneTemplateMap.put(tableName, tableGeneTemplate);
		}

		logger.debug("\nThe generation template map of tables: \n" + tableGeneTemplateMap);
		return tableGeneTemplateMap;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		System.setProperty("com.wolfram.jlink.libdir", 
				"C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink");
		
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(".//test//input//tpch_schema_sf_1.txt");
		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
		List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//tpch_cardinality_constraints_sf_1.txt");
		ComputingThreadPool computingThreadPool = new ComputingThreadPool(4, 20, 0.00001);
		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
		queryInstantiator.iterate();
		List<Parameter> parameters = queryInstantiator.getParameters();
		
		Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
		preprocessor.getPartialOrder();
		Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);
		
		TableGeneTemplate template = tableGeneTemplateMap.entrySet().iterator().next().getValue();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(".//data//template"));
		oos.writeObject(template);
		oos.close();
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(".//data//template"));
		TableGeneTemplate template2 = (TableGeneTemplate)ois.readObject();
		ois.close();
		System.out.println("-----------------------");
		System.out.println(template);
		System.out.println(template2);
	}
}
