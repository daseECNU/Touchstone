package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// count the errors of cardinality constraints for the generated database instance
public class CardinalityTest {

	public static void main(String[] args) throws Exception {
		
		// TPC-H
//		String parametersPath = ".//test//para.txt";
//		String resultsPath = ".//test//results_sf1.txt";
//		int scaleFactor = 1;
//		String ip = "10.11.1.193";
//		String port = "13306";
//		String dbName = "touchstone2_function_test_sf1";
//		String userName = "root";
//		String passwd = "root";
		
		// SSB
		String parametersPath = ".//test//para-ssb.txt";
		String resultsPath = ".//test//results_sf1_ssb.txt";
		int scaleFactor = 1;
		String ip = "10.11.1.193";
		String port = "13306";
		String dbName = "ssb_touchstone";
		String userName = "root";
		String passwd = "root";
		
		Map<Integer, String[]> parameterMap = new HashMap<Integer, String[]>();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(parametersPath)))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				if (!inputLine.contains("Parameter")) {
					continue;
				}
				String[] arr = inputLine.split("id=|, values=|, cardinality=");
				int id = Integer.parseInt(arr[1]);
				String[] values = arr[2].substring(1, arr[2].length() - 1).replaceAll(" ", "").split(",");
				parameterMap.put(id, values);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Connection conn = getDBConnection(ip, port, dbName, userName, passwd);
		Statement stmt = conn.createStatement();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		double expectedCardinality = 0, expectedCardinalitySum = 0, actualCardinality = 0;
		double deviation = 0, deviationSum = 0;
		double error = 0, errorSum = 0, maxError = 0;
		int operatorsNum = 0;
		
		double queryExpectedCardinalitySum = 0, queryDeviationSum = 0;
		double queryErrorSum = 0;
		int queryOperatorsNum = 0;
		List<Double> queryWeightedErrors = new ArrayList<Double>();
		List<Double> queryAverageErrors = new ArrayList<Double>();
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(resultsPath)))) {
			String inputLine = null;
			while ((inputLine = br.readLine()) != null) {
				if (inputLine.matches("[ ]*##[\\s\\S]*") || inputLine.matches("[\\s]*")) {
					continue;
				}

				if (inputLine.startsWith("Query")) {
					if (queryExpectedCardinalitySum != 0) {
						queryWeightedErrors.add(queryDeviationSum / queryExpectedCardinalitySum);
						queryAverageErrors.add(queryErrorSum / queryOperatorsNum);
					}
					queryExpectedCardinalitySum = 0;
					queryDeviationSum = 0;
					queryErrorSum = 0;
					queryOperatorsNum = 0;
					System.out.println(inputLine);
				} else if (inputLine.startsWith("select")) {
					if (inputLine.contains("#")) {
						Pattern p = Pattern.compile("(#[\\d,]+#)");
						Matcher m = p.matcher(inputLine);
						while (m.find()) {
							String paraInfo = m.group();
							String[] arr = paraInfo.substring(1, paraInfo.length() - 1).split(",");
							int id = Integer.parseInt(arr[0]);
							int index = Integer.parseInt(arr[1]);
							String paraStr = parameterMap.get(id)[index];
							if (arr[2].equals("0")) {
								inputLine = inputLine.replaceAll(paraInfo, paraStr);
							} else if(arr[2].equals("1")) {
								inputLine = inputLine.replaceAll(paraInfo, sdf.format(new Date(new Double(paraStr).longValue())));
							}
						}
					}
					System.out.println(inputLine);
					ResultSet rs = stmt.executeQuery(inputLine);
					rs.next();
					actualCardinality = rs.getInt(1);
					System.out.println("actual cardinality:" + actualCardinality);
				} else {
					if (inputLine.contains("###")) {
						inputLine = inputLine.substring(0, inputLine.indexOf("###"));
						expectedCardinality = Double.parseDouble(inputLine);
					} else {
						expectedCardinality = Double.parseDouble(inputLine);
						expectedCardinality = expectedCardinality * scaleFactor;
					}
					System.out.println("expected cardinality:" + expectedCardinality);
					
					deviation = Math.abs(actualCardinality - expectedCardinality);
					error = deviation / expectedCardinality;
					System.out.println("deviation:" + deviation + ", error:" + error + "\n");
					
					expectedCardinalitySum += expectedCardinality;
					queryExpectedCardinalitySum += expectedCardinality;
					
					deviationSum += deviation;
					queryDeviationSum += deviation;
					errorSum += error;
					queryErrorSum += error;
					
					if (error > maxError) {
						maxError = error;
					}
					operatorsNum++;
					queryOperatorsNum++;
				}
			}
			
			// last query
			if (queryExpectedCardinalitySum != 0) {
				queryWeightedErrors.add(queryDeviationSum / queryExpectedCardinalitySum);
				queryAverageErrors.add(queryErrorSum / queryOperatorsNum);
			}
			
			System.out.println("----------------------------------------------");
			System.out.println("global weighted error:" + (deviationSum / expectedCardinalitySum));
			System.out.println("global average error:" + (errorSum / operatorsNum) + "\n");
			
			System.out.println("weighted errors of queries:\n" + queryWeightedErrors);
			System.out.println("average errors of queries:\n" + queryAverageErrors + "\n");
			
			System.out.println("max query weighted error:" + Collections.max(queryWeightedErrors));
			System.out.println("max query average error:" + Collections.max(queryAverageErrors));
			System.out.println("max global error:" + maxError + "\n");
			
			System.out.println("number of cardinality constraints:" + operatorsNum);
			System.out.println("----------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Connection getDBConnection(String ip, String port, String dbName, 
			String userName, String passwd) throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName;
		Class.forName(driver);
		return DriverManager.getConnection(url, userName, passwd);
	}
}
