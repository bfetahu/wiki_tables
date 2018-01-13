package datastruct.wikitable;

import utils.TableCellUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by besnik on 5/22/17.
 */
public class WikiColumnHeader implements Serializable {
    public String column_name;
    public int col_span = 1;
    public int row_span = 1;

    //store the distribution of the data for this column
    private Map<Object, Integer> value_dist;

    public WikiColumnHeader(String column_header_markup) {
        //check if it first contains a colspan
        int[] span = TableCellUtils.getRowColSpan(column_header_markup);
        row_span = span[0];
        col_span = span[1];
        int sub_index = column_header_markup.contains("|") ? column_header_markup.indexOf("|") + 1 : 0;
        column_name = column_header_markup.substring(sub_index).trim();
        column_name = column_name.replaceAll("!", "").trim();
    }

    public String toString() {
        return column_name;
    }

    /**
     * Update the distribution of values for this column.
     *
     * @param cell
     */
    public void updateValueDistribution(WikiTableCell cell) {
        if (value_dist == null) {
            value_dist = new TreeMap<>();
        }

        if (cell.values != null && !cell.values.isEmpty()) {
            for (Map.Entry<String, String> value : cell.values) {
                Integer count = value_dist.get(value.getKey());
                count = count == null ? 0 : count;
                count += 1;
                value_dist.put(value.getKey(), count);
            }
        } else {
            Integer count = value_dist.get(cell.value);
            count = count == null ? 0 : count;
            count += 1;
            value_dist.put(cell.value, count);
        }

    }

    /**
     * Return the sorted entries for the domain of this column.
     *
     * @return
     */
    public List<Map.Entry<Object, Integer>> getSortedColumnDomain() {
        if (value_dist == null) {
            return null;
        }
        List<Map.Entry<Object, Integer>> entries = new ArrayList<>(value_dist.entrySet());
        entries.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        return entries;
    }
}
