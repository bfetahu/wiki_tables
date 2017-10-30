package matcher;

import datastruct.wikitable.WikiColumnHeader;
import datastruct.wikitable.WikiTable;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 7/2/17.
 */
public class TableMatcher {
    /**
     * Returns a list of columns matched across two tables. The matching is done at the instance level in this case.
     *
     * @param table_a
     * @param table_b
     * @return
     */
    public List<Triple<WikiColumnHeader, WikiColumnHeader, Double>> matchTables(WikiTable table_a, WikiTable table_b) {
        List<Triple<WikiColumnHeader, WikiColumnHeader, Double>> matches = new ArrayList<>();

        //establish correspondences between the columns of two tables.
        for (int i = 0; i < table_a.columns[table_a.columns.length - 1].length; i++) {
            WikiColumnHeader col_a = table_a.columns[table_a.columns.length - 1][i];

            for (int j = 0; j < table_b.columns[table_b.columns.length - 1].length; j++) {
                WikiColumnHeader col_b = table_b.columns[table_b.columns.length - 1][j];
                double score = matchColumns(col_a, col_b);
                matches.add(new ImmutableTriple<>(col_a, col_b, score));
            }

        }
        return matches;
    }

    /**
     * Match the overlap of two columns at the instance level.
     *
     * @param col_a
     * @param col_b
     * @return
     */
    public double matchColumns(WikiColumnHeader col_a, WikiColumnHeader col_b) {
        List<Map.Entry<Object, Integer>> col_a_entries = col_a.getSortedColumnDomain();
        List<Map.Entry<Object, Integer>> col_b_entries = col_b.getSortedColumnDomain();
        return 0;
    }
}
