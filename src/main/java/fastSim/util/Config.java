package fastSim.util;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Properties;

public final class Config {

    public static int numIterations = 30; // number to repeat the whole calculation process, meant to get the average runtime
    public static int numHubs = 3000; // number of hubs to be expanded and stored in the disk
    public static double epsilon = 1E-8;
    public static double clip = 1E-4;
    public static double delta = 1E-4;
    //public static int	 numHubsPerIter				= 10;
    public static int eta = 2; // number of times to expand the hubs
    public static double alpha = 0.8; // damping factor
    public static int numClusters = 3;
    public static int maxClusterFaults = 10;
    public static int numRepetitions = 5;

    public static String nodeFile = "node";
    public static String edgeFile = "edge";
    public static String indexDir = "";
    public static String clusterDir = "/Users/zhufanwei/Desktop/fastppv/clusters/";
    public static String communityFile = "";
    public static String outputDir = "";
    public static String queryFile = "query";
    public static String hubType = "";

    public static int resultTop = 50;
    public static int hubTop = 100000; //
    public static int iterations = 10;
    public static int progressiveTopK = 100;
    public static int depth = 8; // maximum path length
    public static double stopRea = 0.001;
    public static int maxNode = 1000;

    static {
        String filePath = "./config.properties";
        File f = new File(filePath);
        if (!f.exists()) {
            System.out.println("Please set the system properties first.");
            System.exit(0);
        }

        //System.out.println("*** config file used: " + f.getAbsolutePath() + " ***");
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(f));
            for (Field field : Config.class.getFields()) {
                if (field.getType().getName().equals("int"))
                    setInt(prop, field);
                else if (field.getType().getName().equals("double"))
                    setDouble(prop, field);
                else if (field.getType().equals(String.class))
                    setString(prop, field);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean hasValidProp(Properties prop, Field field) {
        return prop.getProperty(field.getName()) != null
                && !prop.getProperty(field.getName()).trim().isEmpty();
    }

    private static String getProp(Properties prop, Field field) {
        return prop.getProperty(field.getName()).trim();
    }

    private static void setInt(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field))
            field.set(null, Integer.valueOf(getProp(prop, field)));
    }

    private static void setDouble(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field)) {
            field.set(null, Double.valueOf(getProp(prop, field)));
        }
    }

    private static void setString(Properties prop, Field field) throws Exception {
        if (hasValidProp(prop, field)) {
            field.set(null, getProp(prop, field));
        }
    }

    public static void print() {
        try {
            for (Field field : Config.class.getFields())
                System.out.println(field.getName() + " = " + field.get(null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        print();
    }

}
