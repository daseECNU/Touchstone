package edu.ecnu.touchstone.nonequijoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.run.Touchstone;

public class NonEquiJoinConstraintsReader {

	Logger logger = Logger.getLogger(Touchstone.class);

	// the definition of child nodes should be precede the definition of parent node
	public List<NonEquiJoinConstraint> read(String nonEquiJoinConstraintsInput) {
		List<NonEquiJoinConstraint> nonEquiJoinConstraints = new ArrayList<NonEquiJoinConstraint>();
		Map<Integer, NonEquiJoinConstraint> nonEquiJoinConstraintMap = new HashMap<Integer, NonEquiJoinConstraint>();

		String inputLine = null;
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new 
				FileInputStream(nonEquiJoinConstraintsInput)))) {
			while ((inputLine = br.readLine()) != null) {
				// skip the blank lines and comments
				if (inputLine.matches("[\\s]*") || inputLine.matches("[ ]*##[\\s\\S]*")) {
					continue;
				}
				// convert the input line to lower case and remove the spaces and tabs
				inputLine = inputLine.toLowerCase();
				inputLine = inputLine.replaceAll("[ \\t]+", "");

				String[] arr = inputLine.split(",");
				if (inputLine.matches("[ ]*c[ ]*\\[[\\s\\S^\\]]+\\][ ]*") && arr.length == 4) {
					int id = Integer.parseInt(arr[0].substring(arr[0].indexOf('[') + 1));
					String expression = arr[1].split("@")[0];
					String operator = arr[1].split("@")[1];
					float probability = Float.parseFloat(arr[2]);
					float inputDataSize = Float.parseFloat(arr[3].substring(0, arr[3].indexOf(']')));
					NonEquiJoinConstraint nonEquiJoinConstraint = new NonEquiJoinConstraint(id, expression, 
							operator, probability, inputDataSize);
					nonEquiJoinConstraints.add(nonEquiJoinConstraint);
					nonEquiJoinConstraintMap.put(id, nonEquiJoinConstraint);
				} else if (inputLine.matches("[ ]*r[ ]*\\[[\\s\\S^\\]]+\\][ ]*") && arr.length == 2) {
					int id1 = Integer.parseInt(arr[0].substring(arr[0].indexOf('[') + 1));
					int id2 = Integer.parseInt(arr[1].substring(0, arr[1].indexOf(']')));
					nonEquiJoinConstraintMap.get(id1).getChildren().add(id2);
					nonEquiJoinConstraintMap.get(id1).getChildren().addAll(nonEquiJoinConstraintMap.get(id2).getChildren());
				} else {
					logger.error("\n\tUnable to parse the non-equi join constraint information: " + inputLine);
					System.exit(0);
				}
			}
		} catch (Exception e) {
			logger.error("\n\tError input line: " + inputLine);
			e.printStackTrace();
			System.exit(0);
		}
		logger.debug("\nThe non-equi join constraints: " + nonEquiJoinConstraints);
		return nonEquiJoinConstraints;
	}
	
	// test
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		NonEquiJoinConstraintsReader nonEquiJoinConstraintsReader = new NonEquiJoinConstraintsReader();
		nonEquiJoinConstraintsReader.read(".//test//input//non_equi_join_test.txt");
		nonEquiJoinConstraintsReader.read(".//test//input//function_test_non_equi_join_0.txt");
	}
}
