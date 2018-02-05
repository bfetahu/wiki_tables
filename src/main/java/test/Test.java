package test;

import datastruct.wikitable.WikiTable;
import extractor.TablePrinter;
import io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import utils.TableCellUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(args[0]);
        String out_file = args[1];
        TableCellUtils.init();

        String line;
        Map<String, Integer> tbl_ids = new HashMap<>();
        int table_id = 0;

        while ((line = reader.readLine()) != null) {
            try {
                StringBuffer sb = new StringBuffer();
                String[] tmp = line.split("\t");

                String entity_a = tmp[0];
                String section_a = tmp[1];
                String caption_a = tmp[2];

                String tbl_markup_a = StringEscapeUtils.unescapeJson(tmp[3]);
                if(!tbl_ids.containsKey(tbl_markup_a)){
                    tbl_ids.put(tbl_markup_a, table_id);
                    table_id ++;
                }

                String entity_b = tmp[4];
                String section_b = tmp[5];
                String caption_b = tmp[6];

                String tbl_markup_b = StringEscapeUtils.unescapeJson(tmp[7]);
                if(!tbl_ids.containsKey(tbl_markup_b)){
                    tbl_ids.put(tbl_markup_b, table_id);
                    table_id ++;
                }

                WikiTable tbl_a = new WikiTable(tbl_markup_a);
                WikiTable tbl_b = new WikiTable(tbl_markup_b);

                tbl_a.table_id = tbl_ids.get(tbl_markup_a);
                tbl_b.table_id = tbl_ids.get(tbl_markup_b);

                tbl_a.cleanMarkupTable();
                tbl_a.generateWikiTable();
                tbl_a.linkCellValues();

                tbl_b.cleanMarkupTable();
                tbl_b.generateWikiTable();
                tbl_b.linkCellValues();

                String tbl_a_json = TablePrinter.printTableToJSON(tbl_a);
                String tbl_b_json = TablePrinter.printTableToJSON(tbl_b);

                sb.append(entity_a).append("\t").append(section_a).append("\t").append(caption_a).append("\t").append(tbl_a_json).
                        append("\t").append(entity_b).append("\t").append(section_b).append("\t").append(caption_b).append("\t").append(tbl_b_json).append("\n");
                FileUtils.saveText(sb.toString(), out_file, true);

            } catch (Exception e) {
                System.out.println(line + "\n");
//                e.printStackTrace();
            }
        }
    }

}
