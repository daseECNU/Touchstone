package edu.ecnu.touchstone.run;

import java.util.Arrays;

public class CompressionAlgoTest {
	
	public static void main(String[] args) {
		
		int L = 10000, M = 1000000, k = 10;
		
		int[] array = new int[L];
		
		for (int i = 0; i < L; i++) {
			array[i] = i;
		}
		
		for (int i = L; i <= M; i++) {
			if (Math.random() < (float)L / i) {
				array[(int)(L * Math.random())] = i;
			}
		}
		
		Arrays.sort(array);
		
		for (int i = 1; i < k; i++) {
			int value = (M / k) * i;
			System.out.println(Math.abs((float)Arrays.binarySearch(array, value) / L));
		}
	}
}
