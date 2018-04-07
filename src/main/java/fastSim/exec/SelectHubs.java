package fastSim.exec;

//import fanweizhu.fastSim.core.hubselection.OutDegHub;
//import fanweizhu.fastSim.core.hubselection.PageRankHub;
//import fanweizhu.fastSim.core.hubselection.ProgressiveHub;

import fastSim.core.hubselection.HubSelection;
import fastSim.core.hubselection.InDegHub;
import fastSim.core.hubselection.RandomHub;
import fastSim.util.Config;

public class SelectHubs {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        HubSelection h;
        String hub_type = args[0];
        Config.hubType = hub_type;

        if (hub_type.equals("random"))
            h = new RandomHub(Config.nodeFile, Config.edgeFile);
        else if (hub_type.equals("indeg"))
            h = new InDegHub(Config.nodeFile, Config.edgeFile);
        else {
            System.out.println("Unknown hub selection algorithm!");
            return;
        }
        h.save();
    }

}
