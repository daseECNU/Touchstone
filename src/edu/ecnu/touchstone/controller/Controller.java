package edu.ecnu.touchstone.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.constraintchain.ConstraintChain;
import edu.ecnu.touchstone.constraintchain.ConstraintChainsReader;
import edu.ecnu.touchstone.pretreatment.Preprocessor;
import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.queryinstantiation.ComputingThreadPool;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.queryinstantiation.QueryInstantiator;
import edu.ecnu.touchstone.run.Configurations;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.SchemaReader;
import edu.ecnu.touchstone.schema.Table;

// the controller of the distributed data generator (Touchstone)
// main function: 1. assign the data generation tasks to all data generators, 
//                2. merge the join information of the primary key maintained by data generators
public class Controller {

	// table names stored in the partial order
	private List<String> tablePartialOrder = null;

	// map: table name -> its generation template
	private Map<String, TableGeneTemplate> tableGeneTemplateMap = null;

	// running configurations
	private Configurations configurations = null;

	private Logger logger = null;

	public Controller(List<String> tablePartialOrder, Map<String, TableGeneTemplate> tableGeneTemplateMap, 
			Configurations configurations) {
		super();
		this.tablePartialOrder = tablePartialOrder;
		this.tableGeneTemplateMap = tableGeneTemplateMap;
		this.configurations = configurations;
		logger = Logger.getLogger(Touchstone.class);
	}

	// the clients are linked with the servers of data generators
	// they are used for sending data generation task
	private List<ControllerClient> clients = null;

	// store all 'pkJoinInfo's received from data generators
	private static List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList = null;

	// control the time point of merging the join information of the primary key (pkJoinInfoList)
	private static CountDownLatch countDownLatch = null;

	// set up the server and clients of the controller
	// server: receive the join information of the primary key (pkJoinInfo)
	// clients: send the data generation task
	public void setUpNetworkThreads() {
		new Thread(new ControllerServer(configurations.getControllerPort())).start();

		clients = new ArrayList<ControllerClient>();
		List<String> dataGeneratorIps = configurations.getDataGeneratorIps();
		List<Integer> dataGeneratorPorts = configurations.getDataGeneratorPorts();
		for (int i = 0; i < dataGeneratorIps.size(); i++) {
			ControllerClient client = new ControllerClient(dataGeneratorIps.get(i), dataGeneratorPorts.get(i));
			new Thread(client).start();
			clients.add(client);
		}
	}

	// generate data: tables are generated one by one in accordance with the partial order
	public void geneData() {

		pkJoinInfoList = new ArrayList<Map<Integer, ArrayList<long[]>>>();

		// map: primary key -> reference count
		// it is used to clear the unnecessary join information of primary keys in time
		Map<String, Integer> pkReferenceCountMap = new HashMap<String, Integer>();
		Iterator<Entry<String, TableGeneTemplate>> iterator = tableGeneTemplateMap.entrySet().iterator();
		while (iterator.hasNext()) {
			TableGeneTemplate template = iterator.next().getValue();
			List<String> referencedKeys = template.getReferencedKeys();
			for (int i = 0; i < referencedKeys.size(); i++) {
				if (pkReferenceCountMap.containsKey(referencedKeys.get(i))) {
					int count = pkReferenceCountMap.get(referencedKeys.get(i)) + 1;
					pkReferenceCountMap.put(referencedKeys.get(i), count);
				} else {
					pkReferenceCountMap.put(referencedKeys.get(i), 1);
				}
			}
		}
		logger.info("\n\tThe 'pkReferenceCountMap' (primary key -> reference count) is: " + pkReferenceCountMap);

		// map: primary key (its string representation) -> (combined join statuses -> primary keys list)
		// 'neededPKJoinInfo' -> 'fksJoinInfo'
		Map<String, Map<Integer, ArrayList<long[]>>> neededPKJoinInfo = new HashMap<String, 
				Map<Integer, ArrayList<long[]>>>();

		// wait until all clients are connected to the server of the data generator
		waitClientsConnected();

		long startTime = System.currentTimeMillis();

		logger.info("\n\tStart generating data!");

		for (int i = 0; i < tablePartialOrder.size(); i++) {
			String tableName = tablePartialOrder.get(i);
			TableGeneTemplate template = tableGeneTemplateMap.get(tableName);

			logger.info("\n\tStart generating table " + tableName + "!");

			List<String> referencedKeys = template.getReferencedKeys();
			Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo = 
					new HashMap<String, Map<Integer, ArrayList<long[]>>>();
			for (int j = 0; j < referencedKeys.size(); j++) {
				fksJoinInfo.put(referencedKeys.get(j), neededPKJoinInfo.get(referencedKeys.get(j)));
				int count = pkReferenceCountMap.get(referencedKeys.get(j)) - 1;
				pkReferenceCountMap.put(referencedKeys.get(j), count);
				if (count == 0) {
					// the controller releases the unnecessary join information
					neededPKJoinInfo.remove(referencedKeys.get(j));
				}
			}
			template.setFksJoinInfo(fksJoinInfo);
			logger.info("\n\tThe 'fkJoinInfo' has been set!");
			logger.info("\n\tThe key set of neededPKJoinInfo is: " + neededPKJoinInfo.keySet());
			
			// for experiments
			logger.info("\n\tThe number of constraint chains: " + template.getConstraintChainsNum());
			logger.info("\n\tThe number of constraints in constraint chains: " + template.getConstraintsNum());
			logger.info("\n\tThe number of entries in join information table: " + template.getEntriesNum());
			
			countDownLatch = new CountDownLatch(configurations.getDataGeneratorIps().size());
			for (int j = 0; j < clients.size(); j++) {
				clients.get(j).send(template);
			}
			logger.info(template);
			logger.info("\n\tThe template of " + tableName + " has been successfully sent!");

			// wait for all data generators to return the join information of primary key
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("\n\tThe primary key join information (pkJoinInfo) of all data generators has been received!");

			logger.info("\n\tStart merging 'pkJoinInfoList' ...");
			neededPKJoinInfo.put(template.getPkStr(), 
					JoinInfoMerger.merge(pkJoinInfoList, configurations.getPkvsMaxSize()));
			logger.info("\n\tMerge end!");
			logger.info("\n\tThe key set of neededPKJoinKeyInfo is: " + neededPKJoinInfo.keySet());

			pkJoinInfoList.clear();
		}

		long endTime = System.currentTimeMillis();
		logger.info("\n\tTime of data generation: " + (endTime - startTime) + "ms");
	}

	private void waitClientsConnected() {
		loop : while (true) {
			for (int i = 0; i < clients.size(); i++) {
				if (!clients.get(i).isConnected()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					logger.info("\n\tAll data generators do not startup successful!");
					continue loop;
				}
			}
			break;
		}
	logger.info("\n\tAll data generators startup successful!");
	}

	// it's called by 'ControllerServerHandler' when receiving a 'pkJoinInfo'
	public static synchronized void receivePkJoinInfo(Map<Integer, ArrayList<long[]>> pkJoinInfo) {
		pkJoinInfoList.add(pkJoinInfo);
		countDownLatch.countDown();
	}
	
	// test
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		System.setProperty("com.wolfram.jlink.libdir", 
				"C://Program Files//Wolfram Research//Mathematica//10.0//SystemFiles//Links//JLink");
		
		// TPC-H
//		SchemaReader schemaReader = new SchemaReader();
//		List<Table> tables = schemaReader.read(".//test//input//tpch_schema_sf_1.txt");
//		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
//		List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//tpch_cardinality_constraints_sf_1.txt");
//		ComputingThreadPool computingThreadPool = new ComputingThreadPool(2, 20, 0.00001);
//		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
//		queryInstantiator.iterate();
//		List<Parameter> parameters = queryInstantiator.getParameters();
//		
//		Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
//		List<String> tablePartialOrder = preprocessor.getPartialOrder();
//		Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);
//		Configurations configurations = new Configurations(".//test//touchstone2.conf");
//		Controller controller = new Controller(tablePartialOrder, tableGeneTemplateMap, configurations);
//		controller.setUpNetworkThreads();
//		controller.geneData();
		
		// SSB
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(".//test//input//ssb_schema_sf_1_D.txt");
		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
		List<ConstraintChain> constraintChains = constraintChainsReader.read(".//test//input//ssb_cardinality_constraints_sf_1.txt");
		ComputingThreadPool computingThreadPool = new ComputingThreadPool(2, 20, 0.00001);
		QueryInstantiator queryInstantiator = new QueryInstantiator(tables, constraintChains, null, 20, 0.00001, computingThreadPool);
		queryInstantiator.iterate();
		List<Parameter> parameters = queryInstantiator.getParameters();
		
		Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
		List<String> tablePartialOrder = preprocessor.getPartialOrder();
		Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(1000, 10000);
		Configurations configurations = new Configurations(".//test//touchstone2.conf");
		Controller controller = new Controller(tablePartialOrder, tableGeneTemplateMap, configurations);
		controller.setUpNetworkThreads();
		controller.geneData();
		
	}
}
