package edu.ecnu.touchstone.run;

import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.datagenerator.DataGenerator;

public class RunDataGenerator {

	// args[0]: the path of the configuration file
	// args[1]: the id of the data generator
	public static void main(String[] args) {
		Configurations configurations = new Configurations(args[0]);
		
		PropertyConfigurator.configure(configurations.getLog4jConfFile());
		
		int generatorId = Integer.parseInt(args[1]);
		new Thread(new DataGenerator(configurations, generatorId)).start();
	}
}
