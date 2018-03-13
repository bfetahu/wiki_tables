package datastruct.wikitable;

import org.json.JSONArray;
import org.json.JSONObject;
import utils.TableCellUtils;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by besnik on 5/22/17.
 */
public class WikiTable implements Serializable {
    public int table_id;
    //store the column headers for this table
    public WikiColumnHeader[][] columns;
    //store the table rows, where each row consist of a set of cells.
    public WikiTableCell[][] cells;

    //wiki table markup which we use to generate the tables.
    public String markup;
    public int start;
    public String preceeding_text;
    public String table_caption;

    public WikiTable() {
    }

    public WikiTable(String markup) {
        this.markup = markup;
    }

    public List<String> getTableRows() {
        String[] lines = markup.split("\n+");
        List<String> parsed_lines = new ArrayList<>();
        //first we need to process the markup text and separate the different rows and merge them into single lines.
        StringBuffer sb = new StringBuffer();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];

             /*
             * If the line starts with whitespace, we need to remove since the matching wont work.
             * We do not use trim() since the trailing white space can indicate column which are empty.
             */

            if (line.startsWith(" ")) {
                line = line.replaceAll("^\\s+", "");
            }

            if (line.startsWith("||") || Pattern.compile("^\\|\\s?\\|").matcher(line).find()) {
                line = line.replaceAll("^\\|\\|", "|");
                line = line.replaceAll("^\\|\\s?\\|", "|");
            }

            if (line.isEmpty() || line.equals("|}")) {
                continue;
            }

            //if the line contains !! we need to split to replace it by \t as this is the column delimiter.
            line = line.contains("!!") ? line.replaceAll("!\\s?!", "\t!!") : line;

            //there are fields like {{sortname}} who can contain || and which should not be replaced with \t
            if (line.contains("{{sortname")) {
                line = TableCellUtils.assignTabDelimLine(line);
            } else {
                line = line.contains("|") ? line.replaceAll("\\|\\s?\\|", "\t") : line;
            }

            if (line.startsWith("|+")) {
                parsed_lines.add(line);
                continue;
            }

            if (line.startsWith("|-")) {
                if (sb.length() != 0) {
                    parsed_lines.add(sb.toString());
                    sb.delete(0, sb.length());
                }

                //check if its a new line and contains only formatting clauses.
                // This is important as we can misinterpret it as containing data values.
                // An example is the lines starting like "|-bgcolor="#D0F0C0"
                String tmp_line = TableCellUtils.removeFormattingClauses(line);
                if (tmp_line.equals("|")) {
                    continue;
                }

                //another exception is that the new line has some form of a tag, e.g. |-[key]
                tmp_line = tmp_line.replaceAll("\\|-[0-9a-zA-Z]*", "");
                if (tmp_line.isEmpty()) {
                    continue;
                }
            }

            //in some tables there are no explicit line breaks, however, such line breaks are indicated
            // by rows which are encapsulated within {{ ROW DATA }} clauses, we will limit to only tables
            //which do not contain those explicit line breaks
            if (!markup.contains("|+") && (line.startsWith("{{") || line.equals("}}"))) {
                if (sb.length() != 0) {
                    parsed_lines.add(sb.toString());
                    sb.delete(0, sb.length());
                }
                continue;
            }

            if (line.equals("|-") || line.equals("|--") || line.trim().equals("|-")) {
                continue;
            }

            if (sb.length() != 0 && (line.startsWith("!") || line.contains("|"))) {
                sb.append("\t");
            }
            sb.append(line);
        }
        parsed_lines.add(sb.toString());
        return parsed_lines;
    }

    /**
     * This covers the cases where we are dealing with the first lines and that they start with !
     * or the lines where there are !! which are explicit indicators of the row being a header row.
     *
     * @param line
     * @param index
     * @return
     */
    public boolean isHeaderRow(String line, int index) {
        //in case the table has a caption we can decrement the index by 1
        if (table_caption != null && !table_caption.isEmpty()) {
            index--;
        }
        //if its the first line and starts with ! then we can be sure that this is a header row.
        if (line.startsWith("!") && index == 0) {
            return true;
        }

        //the header always contains !! when there are multiple columns in a line
        if (line.contains("!!") || (line.contains("\t!") && !TableCellUtils.header_match.matcher(line).find()) || (line.contains("!\t") && !TableCellUtils.header_match.matcher(line).find())) {
            return true;
        }

        //in some cases the first line is the table header marked with data row symbols, but usually indicated by multiple ` symbols
        if (index == 0 && line.contains("'''")) {
            return true;
        }

        return false;
    }


    /**
     * Check if a line is a data row.
     *
     * @param line
     * @param index
     * @return
     */
    public boolean isDataRow(String line, int index) {
        /*
            In some cases, there are cells which span across rows or have simple formatting content, hence, they
               may start with !, which should not be an indicator of the row being a header row.

               To ensure that this is exactly a data row, we will check the index of the row and if it further contains
               symbols like | or ||.
         */
        if (line.startsWith("!") && (line.contains("\t|") || line.contains("|\t") || line.contains("||")) && index != 0) {
            return true;
        }
        //there are cases where the line starts with |, however, it is more for formatting issues
        else if (line.startsWith("|") && index == 0 && (line.contains("bgcolor") || line.contains("style"))) {
            return false;
        }
        //in case the row starts with | and  contains || symbols then we are sure this is a data row.
        else if (line.startsWith("|") && (line.contains("\t|") || line.contains("|\t") || line.contains("||"))) {
            return true;
        }
        //in this case we simply look if the line contains the data row column delimiters ||
        else if (index != 0 && line.contains("||")) {
            return true;
        }

        return false;
    }

    /**
     * Extract the table data from the Wiki table markup.
     */
    public void generateWikiTable() {
        List<String> parsed_lines = getTableRows();
        boolean is_data_section = false;

        StringBuffer header = new StringBuffer();
        StringBuffer data_buffer = new StringBuffer();
        for (int i = 0; i < parsed_lines.size(); i++) {
            String line = parsed_lines.get(i);

            //the line starting with |+ represents the table caption.
            if (line.startsWith("|+")) {
                table_caption = line.replace("|+", "");
                continue;
            }

            //check if its a header row
            if (!is_data_section) {
                boolean is_header_row = isHeaderRow(line, i);
                if (is_header_row) {
                    header.append(line).append("\n");
                    continue;
                }

                //if its not a header row check if it is a data row.
                boolean is_data_row = isDataRow(line, i);
                is_data_section = is_data_row;

                //otherwise if it starts with ! then it is a column header row
                if (!is_data_row && line.startsWith("!")) {
                    header.append(line).append("\n");
                    continue;
                }

                if (!is_data_row && !is_header_row && i == 0) {
                    continue;
                }
            }

            //check if the next line is a data row
            if (!is_data_section) {
                if (i != 0) {
                    /*
                    we delete some data column indicators to avoid inaccurate splits. In that case, it might be the case
                    that the data column indicators are only \t delimiters.
                     */
                    if (line.startsWith("|")) {
                        is_data_section = true;
                    } else if (line.contains("\t") && !line.contains("\t!")) {
                        is_data_section = true;
                    }
                }
            }

            if (is_data_section) {
                data_buffer.append(line).append("\n");
            }
        }

        assignColumnHeaders(header.toString().split("\n"));
        assignRows(data_buffer.toString().split("\n"));
    }

    /**
     * Assign the headers to the table. In Wikipedia a table can have multiple rows as headers,
     * where the headers in the upper levels serve as an aggregation of the lower level columns.
     *
     * @param header
     */
    public void assignColumnHeaders(String[] header) {
        //we keep track if we are skipping any line header due to malformed data format
        boolean has_malformed_headers = false;

        columns = new WikiColumnHeader[header.length][];
        int total_cols = 0;
        for (int i = 0; i < header.length; i++) {
            String[] sub_headers = header[i].replace("|-", "").replaceAll("'{2,}", "").trim().split("\t");
            List<WikiColumnHeader> cols = new ArrayList<>();

            for (int j = 0; j < sub_headers.length; j++) {
                String sub_header = sub_headers[j];
                sub_header = TableCellUtils.removeFormattingClauses(sub_header);
                boolean isValidHeader = isValidColumnHeader(sub_header);

                if (!isValidHeader) {
                    continue;
                }

                WikiColumnHeader column = new WikiColumnHeader(sub_header);
                cols.add(column);

                if (i == 0) {
                    total_cols += column.col_span;
                }
            }

            //in case somehow the succeeding header layer has less columns than the previous one,
            //we have to skip as we do not know where to assign the columns,
            //this happens when the second/third column layer is malformed
            if (i != 0 && cols.size() < total_cols) {
                has_malformed_headers = true;
                continue;
            }

            //assign the matrix dimension only in the first column header, there we will know the complete length
            if (i == 0) {
                columns = new WikiColumnHeader[header.length][total_cols];
            }

            int col_pos = 0;
            for (WikiColumnHeader col : cols) {
                if (col.row_span != 1 && col.col_span != 1) {
                    for (int k = i; k < (i + col.row_span); k++) {
                        for (int z = col_pos; z < (col_pos + col.col_span); z++) {
                            columns[k][z] = col;
                        }
                    }
                } else if (col.col_span != 1) {
                    for (int k = col_pos; k < (col_pos + col.col_span); k++) {
                        columns[i][k] = col;
                    }
                } else if (col.row_span != 1) {
                    for (int k = i; k < (i + col.row_span); k++) {
                        columns[k][col_pos] = col;
                    }
                } else {
                    for (int k = col_pos; k < total_cols; k++) {
                        if (columns[i][k] != null) {
                            continue;
                        }
                        columns[i][k] = col;
                        break;
                    }
                }
                col_pos += col.col_span;
            }
        }

        List<WikiColumnHeader[]> headers_filtered = new ArrayList<>();
        if (has_malformed_headers) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null && columns[i][0] != null) {
                    headers_filtered.add(columns[i]);
                }
            }
            WikiColumnHeader[][] tmp_cols = new WikiColumnHeader[headers_filtered.size()][];
            for (int i = 0; i < headers_filtered.size(); i++) {
                tmp_cols[i] = headers_filtered.get(i);
            }
            columns = tmp_cols;
        }

    }

    /**
     * Assign the rows to a table. Each row consists of multiple cells, where each cell is attributed to the
     * one of the columns in the most specialized header in the table.
     * <p>
     * In Wikipedia, tables can have headers with different layers of granularity, where the top layer
     * is the most generalized layer, and the subsequent ones are assigned to the top layers.
     * We use the lowest level to assign the table cells.
     *
     * @param rows
     */
    public void assignRows(String[] rows) {
        if (columns == null) {
            return;
        }
        int max_columns = columns[columns.length - 1].length;
        cells = new WikiTableCell[rows.length][max_columns];

        for (int i = 0; i < rows.length; i++) {
            String row_data = rows[i];
            row_data = row_data.startsWith("||") ? row_data.substring(2) : row_data;
            String[] cells = row_data.split("\t");

            //process all the cells
            for (int j = 0; j < cells.length; j++) {
                //find the cell which has not been assigned a value before
                for (int k = j; k < this.cells[i].length; k++) {
                    if (this.cells[i][k] != null) {
                        continue;
                    }
                    WikiColumnHeader col = columns[columns.length - 1][k];
                    WikiTableCell cell = new WikiTableCell(col);

                    cell.parseValue(cells[j]);
                    cell.linkValues();

                    //check if this value spans across multiple rows and/or columns
                    if (cell.col_span != 1 && cell.row_span != 1) {
                        for (int z = i; z < (i + cell.row_span) && z < cells.length; z++) {
                            for (int l = k; l < (k + cell.col_span) && l < max_columns; l++, j++) {
                                this.cells[z][l] = cell;
                                col.updateValueDistribution(cell);
                            }
                        }
                    } else if (cell.row_span != 1) {
                        //assign the value to the different rows too for the same column
                        for (int z = i; z < (i + cell.row_span) && z < this.cells.length; z++) {
                            this.cells[z][k] = cell;
                            col.updateValueDistribution(cell);
                        }
                    } else if (cell.col_span != 1) {
                        //assign the value to the different rows too for the same column
                        for (int z = k; z < (k + cell.col_span) && z < max_columns; z++, j++) {
                            this.cells[i][z] = cell;
                            col.updateValueDistribution(cell);
                        }
                    } else {
                        this.cells[i][k] = cell;
                        col.updateValueDistribution(cell);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Extract anchor text information from the values.
     */
    public void linkCellValues() {
        for (int i = 0; i < cells.length; i++) {
            for (int j = 0; j < cells[i].length; j++) {
                if (cells[i][j] != null)
                    cells[i][j].linkValues();
            }
        }
    }

    /**
     * Get the category distribution for each column in this table based on the values which link to Wikipedia articles.
     *
     * @param wiki_categories
     * @return
     */
    public Map<Integer, List<Map.Entry<String, Integer>>> getCategoryDistribution(Map<String, Set<String>> wiki_categories) {
        Map<Integer, List<Map.Entry<String, Integer>>> cat_dist = new HashMap<>();

        for (int col_index = 0; col_index < columns[columns.length].length; col_index++) {
            cat_dist.put(col_index, computeCategoryDistribution(wiki_categories, col_index));
        }

        return cat_dist;
    }


    /**
     * Computes the category distribution based on the values which link to specific Wikipedia articles.
     *
     * @param wiki_categories
     * @return
     */
    public List<Map.Entry<String, Integer>> computeCategoryDistribution(Map<String, Set<String>> wiki_categories, int col_index) {
        Map<String, Integer> cat_dist = new HashMap<>();

        for (int row = 0; row < cells.length; row++) {
            WikiTableCell cell = cells[row][col_index];
            if (cell == null) {
                continue;
            }

            for (Map.Entry<String, String> value : cell.values) {
                String article = value.getKey();
                if (!wiki_categories.containsKey(article)) {
                    continue;
                }

                for (String category : wiki_categories.get(article)) {
                    Integer count = cat_dist.get(category);
                    count = count == null ? 0 : count;
                    count += 1;
                    cat_dist.put(category, count);
                }
            }
        }

        //sort the entries
        List<Map.Entry<String, Integer>> sorted_entries = new ArrayList<>(cat_dist.entrySet());
        sorted_entries.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        return sorted_entries;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        //add the header information
        for (int i = 0; i < columns[columns.length - 1].length; i++) {
            WikiColumnHeader col = columns[columns.length - 1][i];
            sb.append(col).append("\t");
        }
        sb.append("\n");

        for (int row = 0; row < cells.length; row++) {
            for (int col = 0; col < cells[0].length; col++) {
                sb.append(cells[row][col]).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString();
    }


    /**
     * Here we assess whether the given column header contains valid value or it is simply a CSS definition.
     *
     * @param col_header
     * @return
     */
    public boolean isValidColumnHeader(String col_header) {
        col_header = col_header.replace("|", "");
        if (col_header.contains("bgcolor")) {
            col_header = col_header.replaceAll("bgcolor=?\"?[0-9a-zA-Z#]*\"?", "").replaceAll("\\W", "").trim();
            return !col_header.isEmpty();

        } else if (col_header.contains("style=")) {
            col_header = col_header.replaceAll("style=?\"?[0-9a-zA-Z#:-]*\"?", "").replaceAll("\\W", "").trim();
            return !col_header.isEmpty();
        }
        return true;
    }

    public void cleanMarkupTable() {
        markup = markup.replaceAll("(?i)\\{+cit(.*?)\\}+", "");
        markup = markup.replaceAll("(?i)\\{+cit((.|\n)*?)\\}+", "");
        markup = markup.replaceAll("<ref(\\s+name=(.*?))?/>", "");
        markup = markup.replaceAll("<ref(\\s+name=(.*?))?>(.*?)</ref>", "");
        markup = markup.replaceAll("data-sort-(.*?)=\"(.*?)\"", "");
        markup = markup.replaceAll("&nbsp.", "");
    }

    /**
     * Loads the table from a structured JSON.
     *
     * @param json
     */
    public void loadFromStructuredJSON(JSONObject json, boolean loadValueDist) {
        this.table_caption = json.getString("caption");
        this.table_id = json.getInt("id");

        //get the table headers.
        JSONArray headers = json.getJSONArray("header");
        int header_len = headers.length();

        columns = new WikiColumnHeader[header_len][];
        for (int i = 0; i < header_len; i++) {
            JSONArray cols = headers.getJSONObject(i).getJSONArray("columns");
            columns[i] = new WikiColumnHeader[cols.length()];

            for (int j = 0; j < cols.length(); j++) {
                JSONObject json_col = cols.getJSONObject(j);
                WikiColumnHeader col = new WikiColumnHeader();
                col.column_name = json_col.getString("name");
                col.col_span = json_col.getInt("col_span");
                col.row_span = json_col.getInt("row_span");

                if (loadValueDist) {
                    col.value_dist = new HashMap<>();
                    JSONArray val_dist = json_col.getJSONArray("value_dist");
                    for (int k = 0; k < val_dist.length(); k++) {
                        JSONObject val_json = val_dist.getJSONObject(k);
                        Object val = val_json.get("value");
                        int count = val_json.getInt("count");

                        col.value_dist.put(val, count);
                    }
                }

                columns[i][j] = col;
            }
        }

        //load the table rows
        JSONArray rows = json.getJSONArray("rows");
        int row_no = rows.length();

        cells = new WikiTableCell[row_no][];
        for (int i = 0; i < row_no; i++) {
            JSONArray row_values = rows.getJSONObject(i).getJSONArray("values");
            cells[i] = new WikiTableCell[row_values.length()];

            for (int k = 0; k < row_values.length(); k++) {
                JSONObject cell_value = row_values.getJSONObject(k);

                String col_name = cell_value.getString("column");
                String val = cell_value.getString("value");

                //get the column header
                WikiColumnHeader col = findColumn(col_name);
                if (col == null) {
                    continue;
                }
                WikiTableCell cell = new WikiTableCell(col);
                cell.value = val;
                cells[i][k] = cell;
            }
        }
    }

    /**
     * Find a column based on its name.
     *
     * @param col_name
     * @return
     */
    public WikiColumnHeader findColumn(String col_name) {
        WikiColumnHeader[] cols = columns[columns.length - 1];
        for (int k = 0; k < cols.length; k++) {
            if (cols[k].column_name.equals(col_name)) {
                return cols[k];
            }
        }
        return null;
    }
}
