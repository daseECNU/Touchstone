package edu.ecnu.touchstone.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// main function: merge the 'pkJoinInfoList'
public class JoinInfoMerger {

	public static Map<Integer, ArrayList<long[]>> merge(List<Map<Integer, 
			ArrayList<long[]>>> pkJoinInfoList, int pkvsMaxSize) {
		Map<Integer, ArrayList<long[]>> mergedPkJoinInfo = pkJoinInfoList.get(0);

		for (int i = 1; i < pkJoinInfoList.size(); i++) {
			Map<Integer, ArrayList<long[]>> pkJoinInfo = pkJoinInfoList.get(i);
			Iterator<Entry<Integer, ArrayList<long[]>>> iterator = pkJoinInfo.entrySet().iterator();

			while (iterator.hasNext()) {
				Entry<Integer, ArrayList<long[]>> entry = iterator.next();
				if (!mergedPkJoinInfo.containsKey(entry.getKey())) {
					mergedPkJoinInfo.put(entry.getKey(), entry.getValue());
				} else {
					ArrayList<long[]> list = mergedPkJoinInfo.get(entry.getKey());
					list.addAll(entry.getValue());
					Collections.shuffle(list);
					if (list.size() > pkvsMaxSize) {
						list = new ArrayList<long[]>(list.subList(0, pkvsMaxSize));
					}
					mergedPkJoinInfo.put(entry.getKey(), list);
				}
			}
		}
		return mergedPkJoinInfo;
	}
}
