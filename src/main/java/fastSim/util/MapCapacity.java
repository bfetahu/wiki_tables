package fastSim.util;

public class MapCapacity {

	public static int compute(int numElements, float loadFactor) {
		return (int)(numElements * (1.0 / loadFactor)) + 16;
	}
	
	public static int compute(int numElements) {
		return compute(numElements, 0.75f);
	}
}
