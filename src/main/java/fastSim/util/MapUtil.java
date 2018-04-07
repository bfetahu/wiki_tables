package fastSim.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;





public class MapUtil {

	public static List<Map.Entry<Integer, Double>> sortMap(Map<Integer, Double> map, int k) {
		//System.out.println("@@@@@@@@@@@@@"+map);
		List<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>(map.entrySet());		
		
		Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
			@Override
			public int compare(Map.Entry<Integer, Double> arg0, Map.Entry<Integer, Double> arg1) {
				return  (arg1.getValue().compareTo(arg0.getValue()));
			}} );
		
		if (k > list.size()) 
			k = list.size();
		
		List<Map.Entry<Integer, Double>> result = new ArrayList<Map.Entry<Integer, Double>>(k);

		for (int i = 0; i < k; i++) {
			result.add(list.get(i));
		}
		
		return result;
	}
	
	
		
}
