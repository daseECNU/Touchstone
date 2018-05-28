package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Configurations {

	// configurations of servers
	// aligned in sequence
	private List<String> ips = new ArrayList<String>();
	private List<String> userNames = new ArrayList<String>();
	private List<String> passwds = new ArrayList<String>();
	private String rootPassword = null;

	// configurations of controller
	private String controllerIp = null;
	private int controllerPort;
	private String controllerRunDir = null;

	// input files
	private String databaseSchemaInput = null;
	private String cardinalityConstraintsInput = null;
	private String nonEquiJoinConstraintsInput = null;

	// configuration of log4j
	private String log4jConfFile = null;

	// configuration of Mathematica
	private String jLinkPath = null;

	// configurations of data generators
	private List<String> dataGeneratorIps = new ArrayList<String>();
	private List<Integer> dataGeneratorPorts = new ArrayList<Integer>();
	private List<Integer> dataGeneratorThreadNums = new ArrayList<Integer>();
	private List<String> dataGeneratorRunDirs = new ArrayList<String>();
	private String dataOutputPath = null;

	// running parameters
	private int queryInstantiationThreadNum;
	private int queryInstantiationMaxIterations, paraInstantiationMaxIterations;
	private double queryInstantiationGlobalRelativeError, paraInstantiationRelativeError;
	private int shuffleMaxNum;
	private int pkvsMaxSize;

	public Configurations(String confFilePath) {
		read(confFilePath);
	}

	private void read(String confFilePath) {
		String inputLine = null;
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(confFilePath)))) {
			while ((inputLine = br.readLine()) != null) {
				// skip the blank lines and comments
				if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
					continue;
				}
				//read the configurations
				String[] arr = inputLine.split(":", 2);
				arr[0] = arr[0].trim();
				arr[1] = arr[1].trim();
				switch (arr[0]) {
				case "IPs of servers":
					String[] tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						ips.add(tmp[i].trim());
					}
					break;
				case "user names of servers":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						userNames.add(tmp[i].trim());
					}
					break;
				case "passwords of servers":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						passwds.add(tmp[i].trim());
					}
					break;
				case "password of root user":
					rootPassword = arr[1];
					break;
				case "IP of controller":
					controllerIp = arr[1];
					break;
				case "port of controller":
					controllerPort = Integer.parseInt(arr[1]);
					break;
				case "running directory of controller":
					controllerRunDir = arr[1];
					break;
				case "database schema":
					databaseSchemaInput = arr[1];
					break;
				case "cardinality constraints":
					cardinalityConstraintsInput = arr[1];
					break;
				case "non-equi join constraints":
					nonEquiJoinConstraintsInput = arr[1];
					break;	
				case "path of log4j.properties":
					log4jConfFile = arr[1];
					break;
				case "path of JLink":
					jLinkPath = arr[1];
					break;
				case "IPs of data generators":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						dataGeneratorIps.add(tmp[i].trim());
					}
					break;
				case "ports of data generators":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						dataGeneratorPorts.add(Integer.parseInt(tmp[i].trim()));
					}
					break;
				case "thread numbers of data generators":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						dataGeneratorThreadNums.add(Integer.parseInt(tmp[i].trim()));
					}
					break;
				case "running directories of data generators":
					tmp = arr[1].split(";");
					for (int i = 0; i < tmp.length; i++) {
						dataGeneratorRunDirs.add(tmp[i].trim());
					}
					break;
				case "data output path":
					dataOutputPath = arr[1];
					break;
				case "thread numbers of query instantiation":
					queryInstantiationThreadNum = Integer.parseInt(arr[1]);
					break;
				case "maximum iterations of query instantiation":
					queryInstantiationMaxIterations = Integer.parseInt(arr[1]);
					break;
				case "global relative error of query instantiation":
					queryInstantiationGlobalRelativeError = Double.parseDouble(arr[1]);
					break;
				case "maximum iterations of parameter instantiation":
					paraInstantiationMaxIterations = Integer.parseInt(arr[1]);
					break;
				case "relative error of parameter instantiation":
					paraInstantiationRelativeError = Double.parseDouble(arr[1]);
					break;
				case "maximum number of shuffle":
					shuffleMaxNum = Integer.parseInt(arr[1]);
					break;
				case "maximum size of PKVs":
					pkvsMaxSize = Integer.parseInt(arr[1]);
					break;
				default:
					System.out.println("Unable to identify the configuration item!\n" + 
							"Error input line: " + inputLine);
					System.exit(0);
				}
			}
		} catch (Exception e) {
			System.out.println("Error input line: " + inputLine);
			e.printStackTrace();
			System.exit(0);
		}
	}

	public List<String> getIps() {
		return ips;
	}

	public List<String> getUserNames() {
		return userNames;
	}

	public List<String> getPasswds() {
		return passwds;
	}

	public String getRootPassword() {
		return rootPassword;
	}

	public String getControllerIp() {
		return controllerIp;
	}

	public int getControllerPort() {
		return controllerPort;
	}

	public String getControllerRunDir() {
		return controllerRunDir;
	}

	public String getDatabaseSchemaInput() {
		return databaseSchemaInput;
	}

	public String getCardinalityConstraintsInput() {
		return cardinalityConstraintsInput;
	}

	public String getNonEquiJoinConstraintsInput() {
		return nonEquiJoinConstraintsInput;
	}

	public String getLog4jConfFile() {
		return log4jConfFile;
	}

	public String getjLinkPath() {
		return jLinkPath;
	}

	public List<String> getDataGeneratorIps() {
		return dataGeneratorIps;
	}

	public List<Integer> getDataGeneratorPorts() {
		return dataGeneratorPorts;
	}

	public List<Integer> getDataGeneratorThreadNums() {
		return dataGeneratorThreadNums;
	}

	public List<String> getDataGeneratorRunDirs() {
		return dataGeneratorRunDirs;
	}

	public String getDataOutputPath() {
		return dataOutputPath;
	}

	public int getQueryInstantiationThreadNum() {
		return queryInstantiationThreadNum;
	}

	public int getQueryInstantiationMaxIterations() {
		return queryInstantiationMaxIterations;
	}

	public int getParaInstantiationMaxIterations() {
		return paraInstantiationMaxIterations;
	}

	public double getQueryInstantiationGlobalRelativeError() {
		return queryInstantiationGlobalRelativeError;
	}

	public double getParaInstantiationRelativeError() {
		return paraInstantiationRelativeError;
	}

	public int getShuffleMaxNum() {
		return shuffleMaxNum;
	}

	public int getPkvsMaxSize() {
		return pkvsMaxSize;
	}

	@Override
	public String toString() {
		return "Configurations [\nips=" + ips + ", \nuserNames=" + userNames + ", \npasswds=" + passwds + ", \ncontrollerIp="
				+ controllerIp + ", controllerPort=" + controllerPort + ", controllerRunDir=" + controllerRunDir
				+ ", \ndatabaseSchemaInput=" + databaseSchemaInput + ", \ncardinalityConstraintsInput="
				+ cardinalityConstraintsInput + ", \nnonEquiJoinConstraintsInput=" + nonEquiJoinConstraintsInput
				+ ", \nlog4jConfFile=" + log4jConfFile + "\njLinkPath=" + jLinkPath + ", \ndataGeneratorIps=" + dataGeneratorIps
				+ ", \ndataGeneratorPorts=" + dataGeneratorPorts + ", \ndataGeneratorThreadNums=" + dataGeneratorThreadNums
				+ ", \ndataGeneratorRunDirs=" + dataGeneratorRunDirs + ", \ndataOutputPath=" + dataOutputPath
				+ ", \nqueryInstantiationThreadNum=" + queryInstantiationThreadNum + ", \nqueryInstantiationMaxIterations="
				+ queryInstantiationMaxIterations + ", \nparaInstantiationMaxIterations=" + paraInstantiationMaxIterations
				+ ", \nqueryInstantiationGlobalRelativeError=" + queryInstantiationGlobalRelativeError
				+ ", \nparaInstantiationRelativeError=" + paraInstantiationRelativeError + ", \nshuffleMaxNum=" 
				+ shuffleMaxNum + ", \npkvsMaxSize=" + pkvsMaxSize + "]";
	}

	// test
	public static void main(String[] args) {
		Configurations configurations = new Configurations(".//test//touchstone.conf");
		System.out.println(configurations);
	}
}
