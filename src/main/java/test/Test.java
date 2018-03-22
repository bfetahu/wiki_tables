package test;

import io.FileUtils;
import representation.CategoryRepresentation;

import java.io.IOException;

/**
 * Created by besnik on 3/12/18.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(args[0]);

        String out_file = args[1];
        StringBuffer sb = new StringBuffer();
        printCategories(out_file, sb, cat);
    }


    /**
     * Write the constructed category taxonomy.
     *
     * @param out_file
     * @param sb
     */
    public static void printCategories(String out_file, StringBuffer sb, CategoryRepresentation cat) {
        if (sb.length() > 10000) {
            FileUtils.saveText(sb.toString(), out_file, true);
            sb.delete(0, sb.length());
        }

        for (String child_label : cat.children.keySet()) {
            sb.append(cat.label).append("\t").append(cat.level).append("\t").append(child_label).append("\t").append(cat.children.get(child_label).level).append("\n");
            printCategories(out_file, sb, cat.children.get(child_label));
        }

        FileUtils.saveText(sb.toString(), out_file, true);
        sb.delete(0, sb.length());
    }
}
