package extractor;

import datastruct.table.WikiColumnHeader;
import datastruct.table.WikiTable;
import datastruct.table.WikiTableCell;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 6/26/17.
 */
public class TablePrinter {
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

            int column_counter = 0;
            for (int j = 0; j < table.columns[i].length; j++) {
                WikiColumnHeader col = table.columns[i][j];
                if (col == null) {
                    continue;
                }
                if (column_counter != 0) {
                    sb.append(",");
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
                column_counter++;
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
