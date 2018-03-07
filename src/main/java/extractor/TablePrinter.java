package extractor;

import datastruct.wikitable.WikiColumnHeader;
import datastruct.wikitable.WikiTable;
import datastruct.wikitable.WikiTableCell;
import io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 6/26/17.
 */
public class TablePrinter {
    public static void main(String[] args) {
        String in_file = args[0];
        String out_file = args[1];

        try {
            printTables(in_file, out_file);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Error processing file %s with message %s.\n", in_file, e.getMessage());
        }
    }

    /**
     * Print the tables in JSON format.
     *
     * @param file
     * @param out_file
     */
    public static void printTables(String file, String out_file) throws Exception {
        BufferedReader reader = FileUtils.getFileReader(file);

        Set<String> entities = FileUtils.readIntoSet("finished_entities.txt", "\n", false);
        entities = entities == null ? new HashSet<>() : entities;
        String line;
        int table_id = 0;
        while ((line = reader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String entity = json.getString("entity");
            if (entities.contains(entity)) {
                continue;
            }

            FileUtils.saveText(entity + "\n", "finished_entities.txt", true);
            System.out.println("Processing tables from entity " + entity);
            StringBuffer sb = new StringBuffer();
            sb.append("{\"entity\":\"").append(StringEscapeUtils.escapeJson(json.getString("entity"))).append("\", \"sections\":[");

            //iterate over all the sections from this entity which have a table
            JSONArray sections = json.getJSONArray("sections");
            for (int i = 0; i < sections.length(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                JSONObject section = sections.getJSONObject(i);
                //add the text of the section for later usage
                sb.append("{\"section\":\"").append(StringEscapeUtils.escapeJson(section.getString("section"))).
                        append("\", \"text\":\"").append(StringEscapeUtils.escapeJson(section.getString("text"))).
                        append("\", \"tables\":[");
                //process all the tables.
                JSONArray tables = section.getJSONArray("tables");
                for (int j = 0; j < tables.length(); j++) {
                    if (j != 0) {
                        sb.append(",");
                    }
                    JSONObject table = tables.getJSONObject(j);
                    String table_markup = table.getString("table_data");
                    WikiTable tbl = new WikiTable(table_markup);

                    try {
                        tbl.table_id = table_id;
                        tbl.cleanMarkupTable();
                        tbl.generateWikiTable();
                        tbl.linkCellValues();

                        String table_json_output = printTableToJSON(tbl);
                        sb.append(table_json_output);
                        table_id++;
                    } catch (Exception e) {
                        FileUtils.saveText(table_markup, "table_print_error.txt", true);
                    }
                }
                sb.append("]}");
            }
            sb.append("]}\n");
            FileUtils.saveText(sb.toString(), out_file, true);
        }
    }


    /**
     * Print the description of the table along with its values.
     *
     * @param table
     * @return
     */
    public static String printTableToJSON(WikiTable table) {
        StringBuffer sb = new StringBuffer();

        sb.append("{\"caption\":\"").append(StringEscapeUtils.escapeJson(table.table_caption)).append("\", ");
        sb.append("\"markup\":\"").append(StringEscapeUtils.escapeJson(table.markup)).append("\",");
        sb.append("\"id\":").append(table.table_id).append(", ");
        sb.append("\"header\":[");
        //first print all the columns, as the table header
        for (int i = 0; i < table.columns.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("{\"level\":").append(i).append(", \"columns\":[");

            for (int j = 0; j < table.columns[i].length; j++) {
                if (j != 0) {
                    sb.append(",");
                }
                WikiColumnHeader col = table.columns[i][j];
                if (col == null) {
                    continue;
                }
                List<Map.Entry<Object, Integer>> value_dist = col.getSortedColumnDomain();
                sb.append("{\"name\":\"").append(StringEscapeUtils.escapeJson(col.column_name)).
                        append("\", \"col_span\":").append(col.col_span).
                        append(", \"row_span\":").append(col.row_span).
                        append(", \"value_dist\":[");

                if (value_dist != null) {
                    for (int k = 0; k < value_dist.size(); k++) {
                        if (k != 0) {
                            sb.append(",");
                        }
                        Map.Entry<Object, Integer> value = value_dist.get(k);
                        sb.append("{\"value\":\"").append(StringEscapeUtils.escapeJson(value.getKey().toString())).
                                append("\",\"count\":").append(value.getValue()).append("}");
                    }
                }
                sb.append("]}");
            }
            sb.append("]}");
        }

        sb.append("], \"rows\":[");
        //print the values for the columns in the lowest level of the table header
        for (int row = 0; row < table.cells.length; row++) {
            if (row != 0) {
                sb.append(",");
            }

            sb.append("{\"row_index\":").append(row).append(", \"values\":[");
            int col_counter = 0;
            for (int col = 0; col < table.cells[row].length; col++) {
                WikiTableCell cell = table.cells[row][col];
                if (cell == null || cell.col_header == null) {
                    continue;
                }
                if (col_counter != 0) {
                    sb.append(",");
                }

                col_counter++;
                sb.append("{\"column\":\"").append(StringEscapeUtils.escapeJson(cell.col_header.column_name)).
                        append("\", \"col_index\":").append(col).
                        append(", \"value\":\"").append(StringEscapeUtils.escapeJson(cell.value)).append("\"");

                //add also the extracted values from the text
                if (cell.values != null && !cell.values.isEmpty()) {
                    sb.append(", \"structured_values\":[");

                    for (int k = 0; k < cell.values.size(); k++) {
                        if (k != 0) {
                            sb.append(",");
                        }

                        Map.Entry<String, String> value = cell.values.get(k);
                        sb.append("{\"structured\":\"").append(StringEscapeUtils.escapeJson(value.getKey())).
                                append("\", \"anchor\":\"").append(StringEscapeUtils.escapeJson(value.getValue())).append("\"}");
                    }
                    sb.append("]");
                }
                sb.append("}");
            }
            sb.append("]}");
        }
        sb.append("]}");
        return sb.toString();
    }
}
