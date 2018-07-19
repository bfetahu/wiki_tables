package test;

import io.FileUtils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by besnik on 3/12/18.
 */
public class Test {
    public static void main(String[] args) throws Exception {

    }

    public static void filterFeatureFiles(String[] args) throws IOException {
        Set<String> entities = FileUtils.readIntoSet(args[0], "\n", false);

        StringBuffer sb = new StringBuffer();
        BufferedReader reader = FileUtils.getFileReader(args[1]);
        String line;
        int idx = 0;
        while ((line = reader.readLine()) != null) {
            if (idx == 0) {
                sb.append(line).append("\n");
                idx++;
                continue;
            }

            String[] data = line.split("\t");
            if (!entities.contains(data[0])) {
                continue;
            }

            sb.append(line).append("\n");
            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), args[2], true);
                sb.delete(0, sb.length());
            }
        }

        FileUtils.saveText(sb.toString(), args[2], true);
    }


    public static void parseTableFeatures(String[] args) throws IOException {
        Set<String> files = new HashSet<>();
        FileUtils.getFilesList(args[0], files);

        String outfile = args[1];
        FileUtils.saveText("entity_a\tentity_b\ttbl_a\ttbl_b\n", outfile);

        for (String file : files) {
            System.out.println("Processing file: " + file);
            BufferedReader reader = FileUtils.getFileReader(file);
            String line;

            StringBuffer sb = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                String[] data = line.split("\t");

                String ea = data[0].replace("entity_a:", "");
                String eb = data[1].replace("entity_b:", "");


                //from here we determine the section in which the table features are computed.
                int start_index = 5;
                if (line.contains("lca_cat")) {
                    for (int i = 5; i < data.length; i++) {
                        if (data[i].contains("lca_cat")) {
                            continue;
                        }
                        start_index = i;
                        break;
                    }
                }

                String tbl_a = data[start_index++].replace("tbl_a:", "");
                String tbl_b = data[start_index++].replace("tbl_b:", "");
                //group the features for the different columns.
                String col_features = "";
                for (int i = start_index; i < data.length; i += 4) {
                    String col_a = data[i].replace("col_a:", "");
                    String col_b = data[i + 1].replace("col_b:", "");
                    String title_sim = data[i + 2].replace("col_title_sim:", "");
                    String abs_sim = data[i + 3].replace("col_abs_sim:", "");


                }

                sb.append(ea).append("\t").append(eb).append("\t");

                if (sb.length() > 1000000) {
                    FileUtils.saveText(sb.toString(), outfile, true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), outfile, true);
        }
    }


}
