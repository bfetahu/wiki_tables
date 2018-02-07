package evaluation;

/**
 * Created by besnik on 2/7/18.
 */
public class CandidatePairStrategies {
    public static void main(String[] args) {
        String cat_rep_sim = "", all_pairs = "", gt_pairs = "", out_dir = "", cat_rep = "", entity_cats = "", option = "";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cat_rep_sim")) {
                cat_rep_sim = args[++i];
            } else if (args[i].equals("-all_pairs")) {
                all_pairs = args[++i];
            } else if (args[i].equals("-gt_pairs")) {
                gt_pairs = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-cat_rep")) {
                cat_rep = args[++i];
            } else if (args[i].equals("-entity_cats")) {
                entity_cats = args[++i];
            } else if (args[i].equals("-option")) {
                option = args[++i];
            }
        }

        //evaluate the different candidate pair strategies.
        if (option.equals("level")) {

        } else if (option.equals("rep_sim")) {

        } else if (option.equals("sim_rank_cat")) {

        } else if (option.equals("sim_rank_entity")) {

        }
    }


}
