package edu.ecnu.touchstone.run;

import java.util.HashMap;
import java.util.Map;

public class Touchstone {

	// args[0]: the path of the configuration file
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Please specify the configuration file for Touchstone!");
			System.exit(0);
		} else {
			Configurations configurations = new Configurations(args[0]);
			Map<String, Integer> ipToIndexMap = new HashMap<String, Integer>();
			for (int i = 0; i < configurations.getIps().size(); i++) {
				ipToIndexMap.put(configurations.getIps().get(i), i);
			}

			// clear the cache of memory
			boolean rootPasswdError = false;
			for (int i = 0; i < configurations.getIps().size(); i++) {
				try {
					RemoteShell.exec(
							configurations.getIps().get(i), "root", configurations.getRootPassword(), 
							"sync; sync; sync; echo 3 > /proc/sys/vm/drop_caches \n " + 
							"echo 0 > /proc/sys/vm/drop_caches");
				} catch (Exception e) {
					rootPasswdError = true;
					break;
				}
			}
			if (!rootPasswdError) {
				System.out.println("Clear the cache of memory for all servers!");
			} else {
				System.out.println("The password of root user is ERROR! Continue to generate data ...");
			}

			// kill all Java processes
			// clear and make all running directories
			RemoteShell.exec(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					"killall java \n " + 
					"rm -rf " + configurations.getControllerRunDir() + " \n " + 
					"mkdir -p " + configurations.getControllerRunDir());
			for (int i = 0; i < configurations.getDataGeneratorRunDirs().size(); i++) {
				RemoteShell.exec(
						configurations.getDataGeneratorIps().get(i), 
						configurations.getUserNames().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						configurations.getPasswds().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						"killall java \n " + 
						"rm -rf " + configurations.getDataGeneratorRunDirs().get(i)  + " \n " + 
						"mkdir -p " + configurations.getDataGeneratorRunDirs().get(i));
			}
			System.out.println("All running directories were successfully made!");


			// copy all the required files into the running directory
//			String runControllerPath = ".//test//RunController.jar";
//			String runDataGeneratorPath = ".//test//RunDataGenerator.jar";
//			String inputPath = ".//test//input";
//			String libPath = ".//test//lib";
			String runControllerPath = ".//RunController.jar";
			String runDataGeneratorPath = ".//RunDataGenerator.jar";
			String inputPath = ".//input";
			String libPath = ".//lib";

			// controller: RunController.jar, configuration file, input, lib
			RemoteShell.uploadFile(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					runControllerPath, 
					configurations.getControllerRunDir());
			RemoteShell.uploadFile(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					args[0], 
					configurations.getControllerRunDir());
			RemoteShell.uploadDirectory(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					inputPath, 
					configurations.getControllerRunDir());
			RemoteShell.uploadDirectory(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					libPath, 
					configurations.getControllerRunDir());

			// data generator: RunDataGenerator.jar, configuration file
			for (int i = 0; i < configurations.getDataGeneratorRunDirs().size(); i++) {
				RemoteShell.uploadFile(
						configurations.getDataGeneratorIps().get(i), 
						configurations.getUserNames().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						configurations.getPasswds().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						runDataGeneratorPath, 
						configurations.getDataGeneratorRunDirs().get(i));
				RemoteShell.uploadFile(
						configurations.getDataGeneratorIps().get(i), 
						configurations.getUserNames().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						configurations.getPasswds().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						args[0], 
						configurations.getDataGeneratorRunDirs().get(i));
				RemoteShell.uploadDirectory(
						configurations.getDataGeneratorIps().get(i), 
						configurations.getUserNames().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						configurations.getPasswds().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						libPath, 
						configurations.getDataGeneratorRunDirs().get(i));
			}
			System.out.println("All required files were successfully copied!");

			// set up the controller and data generators
			int index1 = args[0].lastIndexOf("/");
			int index2 =  args[0].lastIndexOf("\\");
			int index = index1 > index2 ? index1 : index2;
			String configurationFileName = args[0].substring(index + 1);

			RemoteShell.exec(
					configurations.getControllerIp(), 
					configurations.getUserNames().get(ipToIndexMap.get(configurations.getControllerIp())), 
					configurations.getPasswds().get(ipToIndexMap.get(configurations.getControllerIp())), 
					"cd " + configurations.getControllerRunDir()   + " \n " +
					"mkdir log \n" + 
					"nohup java -jar RunController.jar .//" + configurationFileName + " > nohup.txt &");
			for (int i = 0; i < configurations.getDataGeneratorRunDirs().size(); i++) {
				RemoteShell.exec(
						configurations.getDataGeneratorIps().get(i), 
						configurations.getUserNames().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						configurations.getPasswds().get(ipToIndexMap.get(configurations.getDataGeneratorIps().get(i))), 
						"cd " + configurations.getDataGeneratorRunDirs().get(i)   + " \n " + 
						"mkdir log \n" + 
						"mkdir -p " + configurations.getDataOutputPath() + "\n" + 
						"nohup java -jar RunDataGenerator.jar .//" + configurationFileName + " " + i + " > nohup.txt &");
			}
			System.out.println("Controller and data generators startup successful!");
		}
	}
}
