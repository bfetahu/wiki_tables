package test;

import datastruct.WikiAnchorGraph;
import io.FileUtils;

import java.io.IOException;

/**
 * Created by besnik on 2/9/18.
 */
public class TestSimRank {
    public static void main(String[] args) throws IOException {
        /**
         *      G.add_edges_from([('a','b'), ('b', 'c'), ('c','a'), ('c','d')])
         S(a,b) = r * (S(b,a)+S(b,c)+S(c,a)+S(c,c))/(2*2) = 0.9 * (0.6538+0.6261+0.6261+1)/4 = 0.6538,
         G.add_edges_from([('1','2'), ('1', '4'), ('2','3'), ('3','1'), ('4', '5'), ('5', '4')])


         */

//        String edges = "1\t2\n1\t4\n2\t3\n3\t1\n4\t5\n5\t4\n";
//        FileUtils.saveText(edges, "test.txt");

        WikiAnchorGraph w = new WikiAnchorGraph();
        w.loadInDegreeAnchorGraph("/Users/besnik/Desktop/test.txt", "");
//        w.readEntityFilterFiles(args[2]);
        w.initialize();
        w.computeGraphSimRank(0.8, 2);
        w.writeSimRankScores("test_out.txt");
    }
}
