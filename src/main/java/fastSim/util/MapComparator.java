package fastSim.util;

import java.util.Comparator;
import java.util.Map;

public class MapComparator implements Comparator<Integer> {
	Map<Integer,Double> base;
	public MapComparator(Map<Integer,Double> base){
		this.base = base;
	}
	@Override
	public int compare(Integer arg0, Integer arg1) {
		// TODO Auto-generated method stub
		if(base.get(arg0)>=base.get(arg1))
			return -1;
		else 
			return 1;
	}
	
}
