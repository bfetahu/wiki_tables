package test;

import utils.FileUtils;

import java.io.BufferedReader;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        BufferedReader reader = FileUtils.getFileReader("/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv");
        String line;

        StringBuffer sb = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\t");
            if (tmp.length != 2) {
                continue;
            }

            tmp[0] = tmp[0].replaceAll("\\|(.*?)$", "");
            tmp[1] = tmp[1].replaceAll("\\|(.*?)$", "");

            sb.append(tmp[0]).append("\t").append(tmp[1]).append("\n");

            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv.txt", true);
                sb.delete(0, sb.length());
            }
        }

        FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv.txt", true);
    }
}
