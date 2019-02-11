package datastruct.table;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

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
    public String table_caption;

    public String entity;
    public String section;


    public int getNumColumns() {
        if (columns == null) {
            return 0;
        }
        return columns[columns.length - 1].length;
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
     * Loads the table from a structured JSON.
     *
     * @param json
     */
    public void loadFromStructuredJSON(JSONObject json, boolean loadValueDist, boolean loadCellValues) {
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
                if (cols.get(j).toString().equals("null")) {
                    continue;
                }
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

        if (loadCellValues) {
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

    public void initializeCells(int num_rows) {
        cells = new WikiTableCell[num_rows][getNumColumns()];
    }


    /**
     * Add the cell into the matrix.
     *
     * @param cell
     * @param row_idx
     * @param col_idx
     * @return
     */
    public int addCellValue(WikiTableCell cell, int row_idx, int col_idx) {
        if (cell.col_span == 1 && cell.row_span == 1) {
            int k = findNextFreeColumnSlot(row_idx, col_idx);
            cells[row_idx][k] = cell;
            col_idx = k + 1;
        } else if (cell.col_span != 1 && cell.row_span != 1) {
            //get first the next free column spot
            int k = findNextFreeColumnSlot(row_idx, col_idx);
            for (int j = k; j < k + cell.col_span && j < columns[0].length; j++) {
                for (int i = row_idx; i < row_idx + cell.row_span & i < cells.length; i++) {
                    cells[i][j] = cell;
                }
            }
            col_idx = k + cell.col_span;
        } else if (cell.col_span != 1) {
            int k = findNextFreeColumnSlot(row_idx, col_idx);
            for (int j = k; j < k + cell.col_span && j < columns[0].length; j++) {
                cells[row_idx][j] = cell;
            }
            col_idx = k + cell.col_span;
        } else if (cell.row_span != 1) {
            int k = findNextFreeColumnSlot(row_idx, col_idx);
            for (int i = row_idx; i < row_idx + cell.row_span & i < cells.length; i++) {
                cells[i][k] = cell;
            }
            col_idx = k + 1;
        }
        cell.col_header.updateValueDistribution(cell);
        return col_idx;
    }

    /**
     * For a given row check where is the next free column slot.
     *
     * @param row_idx
     * @param k
     * @return
     */
    public int findNextFreeColumnSlot(int row_idx, int k) {
        if (cells[row_idx][k] == null) {
            return k;
        } else {
            for (int j = k; j < getNumColumns(); j++) {
                if (cells[row_idx][j] == null) {
                    return j;
                }
            }
        }
        return k;
    }


    /**
     * Extract the column headers from the HTML representation of a table.
     *
     * @param table_rows
     * @return
     */
    public void setColumnHeaders(List<Element> table_rows) {
        List<Element> rows = table_rows.stream().filter(tr -> tr.select("td").isEmpty()).collect(Collectors.toList());

        //in some cases the header rows are not clearly indicated, and they are marked as data rows, in those cases we take the first row.
        boolean flawed_header = false;
        if (rows.isEmpty()) {
            rows = table_rows.stream().filter(tr -> !tr.select("td").isEmpty()).collect(Collectors.toList());
            rows = rows.subList(0, 1);
            flawed_header = true;
        }
        columns = new WikiColumnHeader[rows.size()][];
        int num_cols = 0;
        for (int i = 0; i < rows.size(); i++) {
            Elements th_headers = !flawed_header ? rows.get(i).select("th") : rows.get(i).select("td");

            //in the first header row we get the exact amount of columns we need.
            if (i == 0) {
                for (Element elm : th_headers) {
                    if (elm.hasAttr("colspan")) {
                        num_cols += Integer.parseInt(elm.attr("colspan"));
                    } else {
                        num_cols++;
                    }
                }
                for (int row_idx = 0; row_idx < columns.length; row_idx++) {
                    columns[row_idx] = new WikiColumnHeader[num_cols];
                }
            }


            //column indexer
            int k = 0;
            for (int j = 0; j < th_headers.size(); j++) {
                Element th_header = th_headers.get(j);
                String col_name = th_header.text();

                int col_span = 1, row_span = 1;
                if (th_header.hasAttr("colspan")) {
                    col_span = Integer.parseInt(th_header.attr("colspan"));
                }
                if (th_header.hasAttr("rowspan")) {
                    row_span = Integer.parseInt(th_header.attr("rowspan"));
                }

                WikiColumnHeader col = new WikiColumnHeader(col_name, row_span, col_span);
                k = addColumnToHeader(col, columns, i, k);
            }
        }
    }

    /**
     * Add the column into the table header.
     *
     * @param col
     * @param cols
     * @param row_idx
     * @param col_idx
     */
    private static int addColumnToHeader(WikiColumnHeader col, WikiColumnHeader[][] cols, int row_idx, int col_idx) {
        if (col.col_span == 1 && col.row_span == 1) {
            int k = locateFreeColumnSlot(cols, row_idx, col_idx);
            cols[row_idx][k] = col;
            col_idx = k + 1;
        } else if (col.col_span != 1 && col.row_span != 1) {
            int k = locateFreeColumnSlot(cols, row_idx, col_idx);
            for (int j = k; j < k + col.col_span && j < cols[row_idx].length; j++) {
                for (int i = row_idx; i < row_idx + col.row_span & i < cols.length; i++) {
                    cols[i][j] = col;
                }
            }
            col_idx = k + col.col_span;
        } else if (col.col_span != 1) {
            int k = locateFreeColumnSlot(cols, row_idx, col_idx);
            for (int j = k; j < k + col.col_span && j < cols[row_idx].length; j++) {
                cols[row_idx][j] = col;
            }
            col_idx = k + col.col_span;
        } else if (col.row_span != 1) {
            int k = locateFreeColumnSlot(cols, row_idx, col_idx);
            for (int i = row_idx; i < row_idx + col.row_span && i < cols.length; i++) {
                cols[i][k] = col;
            }
            col_idx = k + 1;
        }
        return col_idx;
    }


    /**
     * Find the column slot that is not occupied already.
     *
     * @param cols
     * @param row
     * @param col
     * @return
     */
    private static int locateFreeColumnSlot(WikiColumnHeader[][] cols, int row, int col) {
        if (cols[row][col] == null) {
            return col;
        }

        for (int i = col; i < cols[row].length; i++) {
            if (cols[row][i] == null) {
                return i;
            }
        }
        return col;
    }
}
