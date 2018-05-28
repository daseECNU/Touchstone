package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class CodeLineCounter {

	public static void main(String[] args) throws Exception {

		File[] packageFiles = new File(".//src//edu//ecnu//touchstone").listFiles();
		ArrayList<File> codeFiles = new ArrayList<File>();
		for (int i = 0; i < packageFiles.length; i++) {
			codeFiles.addAll(Arrays.asList(packageFiles[i].listFiles()));
		}
		int count = 0;
		for (int i = 0; i < codeFiles.size(); i++) {
			if (!codeFiles.get(i).getName().contains("java")) {
				continue;
			}
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(codeFiles.get(i))));
			while (br.readLine() != null) {
				count++;
			}
			br.close();
		}
		System.out.println(count);
	}
}
