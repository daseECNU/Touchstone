package edu.ecnu.touchstone.datagenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.controller.JoinInfoMerger;
import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.run.Configurations;
import edu.ecnu.touchstone.run.Statistic;
import edu.ecnu.touchstone.run.Touchstone;

// in practice, multiple data generators are deployed in general
// main functions: generate data, maintain join information of the primary key
public class DataGenerator implements Runnable {

	private Logger logger = null;

	// running configurations
	private Configurations configurations = null;
	
	// the ID of the data generator
	private int generatorId;

	public DataGenerator(Configurations configurations, int generatorId) {
		super();
		this.configurations = configurations;
		this.generatorId = generatorId;
		logger = Logger.getLogger(Touchstone.class);
	}

	// each data generation thread has a blocking queue for transmitting 'TableGeneTemplate'
	private static List<ArrayBlockingQueue<TableGeneTemplate>> templateQueues = null;

	// the client linked with the server of the controller
	// it is used for sending 'pkJoinInfo'
	private DataGeneratorClient client = null;

	// store all 'pkJoinInfo's maintained by data generation threads
	// firstly, merge locally, then send to controller
	private static List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList = null;
	
	// control the time point of merging the join information of the primary key (pkJoinInfoList)
	private static CountDownLatch countDownLatch = null;

	@Override
	public void run() {

		pkJoinInfoList = new ArrayList<Map<Integer, ArrayList<long[]>>>();
		int threadNum = configurations.getDataGeneratorThreadNums().get(generatorId);
		countDownLatch = new CountDownLatch(threadNum);

		setUpDataGenerationThreads();
		setUpNetworkThreads();

		while (true) {
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			logger.info("\n\tStart merging 'pkJoinInfoList' ...");
			client.send(JoinInfoMerger.merge(pkJoinInfoList, configurations.getPkvsMaxSize()));
			logger.info("\n\tMerge end!");
			logger.info("\n\tThe fkMissCount: " + Statistic.fkMissCount);

			pkJoinInfoList.clear();
			countDownLatch = new CountDownLatch(threadNum);
		}
	}

	private void setUpDataGenerationThreads() {
		// 'localThreadNum' is the number of threads on this node
		int localThreadNum = configurations.getDataGeneratorThreadNums().get(generatorId);
		// 'allThreadNum' is the number of threads on the all nodes
		// 'count' is the number of threads on the previous (generatorId - 1) nodes
		int allThreadNum = 0, count = 0;
		for (int i = 0; i < configurations.getDataGeneratorThreadNums().size(); i++) {
			allThreadNum += configurations.getDataGeneratorThreadNums().get(i);
			if (i == generatorId - 1) {
				count = allThreadNum;
			}
		}
		
		// set up all data generation threads
		templateQueues = new ArrayList<ArrayBlockingQueue<TableGeneTemplate>>();
		for (int i = 0; i < localThreadNum; i++) {
			templateQueues.add(new ArrayBlockingQueue<TableGeneTemplate>(1));
			int threadId = count + i;
			new Thread(new DataGenerationThread(templateQueues.get(i), threadId, allThreadNum, 
					configurations.getDataOutputPath())).start();
		}
		logger.info("\n\tAll data generation threads startup successful!");
	}

	// set up the server and client of the data generator
	// server: receive the data generation task (TableGeneTemplate)
	// client: send the maintained join information of the primary key
	private void setUpNetworkThreads() {
		int serverPort = configurations.getDataGeneratorPorts().get(generatorId);
		String controllerIp = configurations.getControllerIp();
		int controllerPort = configurations.getControllerPort();

		new Thread(new DataGeneratorServer(serverPort)).start();
		client = new DataGeneratorClient(controllerIp, controllerPort);
		new Thread(client).start();
	}
	
	// it's called by 'DataGeneratorServerHandler' when receiving a data generation task (template)
	// 'synchronized' is no needed because 'template' must have been sent one by one (last one has been processed)
	public static void addTemplate(TableGeneTemplate template) {
		// to avoid the interference among data generation threads, we assign a deep copy of 'template' to each thread
		// note: 'fksJoinInfo' only has a shallow copy
		try {
			for (int i = 0; i < templateQueues.size(); i++) {
				templateQueues.get(i).put(new TableGeneTemplate(template));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// collect all 'pkJoinInfo's  maintained by data generation threads
	public static synchronized void addPkJoinInfo(Map<Integer, ArrayList<long[]>> pkJoinInfo) {
		pkJoinInfoList.add(pkJoinInfo);
		countDownLatch.countDown();
	}
	
	// test
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		Configurations configurations = new Configurations(".//test//touchstone2.conf");
		// in a JVM, you can only have one data generator
		// because there are some static attributes in class 'DataGenerator'
		for (int i = 0; i < configurations.getDataGeneratorIps().size(); i++) {
			new Thread(new DataGenerator(configurations, i)).start();
		}
	}
}

class DataGenerationThread implements Runnable {

	private BlockingQueue<TableGeneTemplate> templateQueue = null;
	private int threadId;
	// the number of all threads in all data generators
	private int threadNum;
	private String dataOutputPath = null;

	public DataGenerationThread(BlockingQueue<TableGeneTemplate> templateQueue, int threadId,
			int threadNum, String dataOutputPath) {
		super();
		this.templateQueue = templateQueue;
		this.threadId = threadId;
		this.threadNum = threadNum;
		this.dataOutputPath = dataOutputPath;
	}

	@Override
	public void run() {
		try {
			StringBuilder sb = new StringBuilder();
			while (true) {
				TableGeneTemplate template = templateQueue.take();
				long tableSize = template.getTableSize();
				File outputFile = new File(dataOutputPath + "//" + template.getTableName() + "_" + threadId + ".txt");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new 
						FileOutputStream(outputFile), "UTF-8"));
				for (long uniqueNum = threadId; uniqueNum < tableSize; uniqueNum += threadNum) {
					String[] tuple = template.geneTuple(uniqueNum);
					for (int i = 0; i < tuple.length - 1; i++) {
						sb.append(tuple[i]);
						sb.append(",");
					}
					sb.append(tuple[tuple.length - 1]);
					sb.append("\n");
					bw.write(sb.toString());
					sb.setLength(0);
				}
				DataGenerator.addPkJoinInfo(template.getPkJoinInfo());
				bw.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

