package extractor;

import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by besnik on 3/27/18.
 */
public class AbstractExtractor {
    public static void main(String[] args) throws IOException {
        String in_dir = args[0];
        String out_file = args[1];

        //do a parallel faster parse
        List<String> entities = new ArrayList<>();
        BufferedReader reader = FileUtils.getFileReader(in_dir);
        StringBuffer sb = new StringBuffer();
        String line;
        boolean is_data_region = false;
        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (line.equals("<links>") || line.startsWith("<sublink") || line.equals("</links>") || line.startsWith("<url")) {
                continue;
            }

            if (line.equals("<doc>")) {
                is_data_region = true;
                if (sb.length() != 0) {
                    entities.add(sb.toString());
                    sb.delete(0, sb.length());
                }

                if (entities.size() > 10000) {
                    parseAbstract(entities, out_file);
                }
                continue;
            } else if (line.equals("</doc>")) {
                continue;
            }
            if (is_data_region) {
                line = line.replaceAll("<(.*?)>", "");
                sb.append(line).append("\t");
            }
        }

        if (sb.length() != 0) {
            entities.add(sb.toString());
        }

        parseAbstract(entities, out_file);
    }

    public static void parseAbstract(List<String> abstracts, String out_file) {
        StringBuffer sb = new StringBuffer();
        for (String entity_abs : abstracts) {
            sb.append(entity_abs).append("\n");

        }
        FileUtils.saveText(sb.toString(), out_file, true);
        abstracts.clear();
    }
}
