package extractor;

import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 6/27/18.
 */
public class DBpediaGraph {
    public static void main(String[] args) throws IOException {
        Set<String> files = new HashSet<>();
        FileUtils.getFilesList(args[0], files);

        Map<String, Integer> nodes = new HashMap<>();
        int node_idx = 0;

        //output the edges for the DBpedia graph.
        StringBuffer sb = new StringBuffer();
        String out_file = args[1];
        for (String file : files) {
            System.out.printf("Processing file %s.\n", file);
            BufferedReader reader = FileUtils.getFileReader(file);
            String line;

            while ((line = reader.readLine()) != null) {
                //skip lines that contain literal values
                if (!line.endsWith("> .")) {
                    continue;
                }

                String[] data = line.split("\\s+");

                String subject = data[0];
                String object = data[1];


                if (!nodes.containsKey(subject)) {
                    nodes.put(subject, node_idx);
                    node_idx++;
                }

                if (!nodes.containsKey(object)) {
                    nodes.put(object, node_idx);
                    node_idx++;
                }

                sb.append(nodes.get(subject)).append(" ").append(nodes.get(object)).append("\n");
                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), out_file, true);
        }

        //output the node index too
        sb.delete(0, sb.length());
        nodes.keySet().forEach(node -> sb.append(node).append("\t").append(nodes.get(node)).append("\n"));
        FileUtils.saveText(sb.toString(), args[2]);
    }
}
