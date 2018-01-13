package datastruct.wikitable;

import utils.TableCellUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 5/22/17.
 */
public class WikiTableCell implements Serializable {
    //check if the value spans across rows and/or columns
    public int col_span = 1;
    public int row_span = 1;

    //the column to which this cell is assigned
    public WikiColumnHeader col_header;

    //the value assigned to this cell.
    public String value;

    //hold the parsed and interlinked values
    public List<Map.Entry<String, String>> values;

    public WikiTableCell(WikiColumnHeader col_header) {
        this.col_header = col_header;
    }

    /**
     * Parse the value for a table cell. Check first if this value spans across rows and columns.
     *
     * @param value
     */
    public void parseValue(String value) {
        if (value.startsWith("|")) {
            value = value.replaceAll("^(\\s?\\|)+", "");
        }
        int[] span = TableCellUtils.getRowColSpan(value);
        value = TableCellUtils.removeFormattingClauses(value);
        row_span = span[0];
        col_span = span[1];

        value = value.replaceAll("^!\\|?", "");
        value = value.replaceAll("^\\|+", "");
        value = value.replaceAll("'{2,}", "");
        this.value = value.trim().intern();
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(value.toString());
        if (values != null && !values.isEmpty()) {
            sb.append(" == ");
            values.forEach(s -> sb.append(s).append(";"));
        }
        return sb.toString();
    }

    /**
     * For a given cell value, we parse the values by removing the anchor text and replacing it with the links to the entity pages.
     * [[Confederation of African Athletics|Africa]] {{([[List of African records in athletics|records]])}}
     */
    public void linkValues() {
        if (values != null && !values.isEmpty()) {
            return;
        }
        values = new ArrayList<>();
        //in this case we look for markup which contains links to other Wikipedia articles.
        int pos = 0;
        if (value.contains("[[") || value.contains("{{")) {
            pos = TableCellUtils.extractAnchorData(value, values, pos);
        }
        if (value.contains("{{")) {
            if (value.contains("[[")) {
                String value_tmp = value.replaceAll("\\{+|\\}+", "");
                TableCellUtils.extractAnchorData(value_tmp, values, pos);
            } else {
                TableCellUtils.extractBracketedValue(value, values, pos);
            }
        }
    }
}
