package fastSim.util;

import java.util.Map;

public class PrintInfor {
	
	public static void printDoubleMap(Map<Integer, Map<Integer,Double>> map, String info){
		System.out.println(info);
		for(Map.Entry<Integer, Map<Integer,Double>> e: map.entrySet()){
			
			for(Map.Entry<Integer, Double> hub: e.getValue().entrySet())
				System.out.print(e.getKey()+ " <"+ hub.getKey() +" "+ hub.getValue() + ">");
			System.out.println();
		}
	}
	
}
