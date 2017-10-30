package test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        URLConnection connection = new URL("http://conservapedia.com/index.php?title=Barack_Hussein_Obama&action=history").openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
        connection.connect();

        BufferedReader r  = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));

        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = r.readLine()) != null) {
            sb.append(line);
        }
        System.out.println(sb.toString());
//        String test_markup = FileUtils.readText("/Users/besnik/Desktop/test.txt");
//
//        WikiTable tbl = new WikiTable(test_markup);
//        tbl.cleanMarkupTable();
//        tbl.generateWikiTable();
//
//        System.out.println(TablePrinter.printTableToJSON(tbl));
    }
}
