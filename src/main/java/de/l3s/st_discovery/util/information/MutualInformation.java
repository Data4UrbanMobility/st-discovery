package de.l3s.st_discovery.util.information;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

public class MutualInformation {

    public static double mutualInformation(int[] lhs, int[] rhs) {
        Map<Integer, Double> p_x = new HashMap<>();
        Map<Integer, Double> p_y = new HashMap<>();
        Map<Pair<Integer, Integer>, Double> p_x_y = new HashMap<>();

        //calculate probabilities
        for (int i=0; i<lhs.length; ++i) {
            int x = lhs[i];
            int y = rhs[i];
            Pair<Integer, Integer> x_y = new ImmutablePair<>(x,y);

            if (!p_x.containsKey(x)) {
                p_x.put(x,0.0);
            }
            p_x.put(x, p_x.get(x)+1);

            if (!p_y.containsKey(y)) {
                p_y.put(y, 0.0);
            }
            p_y.put(y, p_y.get(y)+1);

            if(!p_x_y.containsKey(x_y)) {
                p_x_y.put(x_y, 0.0);
            }
            p_x_y.put(x_y, p_x_y.get(x_y)+1);
        }

        for (Integer x: p_x.keySet()) {
            p_x.put(x, p_x.get(x) / ((double)lhs.length));
        }

        for (Integer y: p_y.keySet()) {
            p_y.put(y, p_y.get(y) / ((double)lhs.length));
        }

        for (Pair<Integer, Integer> x_y: p_x_y.keySet()) {
            p_x_y.put(x_y, p_x_y.get(x_y) / ((double)lhs.length));
        }


        double mi = 0;
        //calculate mutual information
        for(Pair<Integer, Integer> x_y: p_x_y.keySet()) {
            mi += p_x_y.get(x_y) * Math.log(p_x_y.get(x_y) / (p_x.get(x_y.getLeft()) * p_y.get(x_y.getRight())));
        }

        //information is encoded in bits, use log with basis 2
        mi /= Math.log(2);

        return mi;
    }
    

}
