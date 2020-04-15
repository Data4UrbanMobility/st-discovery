package de.l3s.st_discovery.model;

import de.l3s.st_discovery.graph.Subgraph;
import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;
import de.l3s.st_discovery.output.DependencyRecord;
import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import de.l3s.st_discovery.util.db.PostgreDB;
import de.l3s.st_discovery.util.information.MutualInformation;
import de.l3s.st_discovery.util.misc.ProgressBar;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.*;

public class DependencyCalculator extends Configurable {

    private double th_d, th_sim;
    private String outlierTable, timeColumn, subgraphName,subgraphTable, inputTable, graphTable;


    public static void registerConfigEntries(ConfigurationParser cp) {
        cp.addDoubleOption("thd", "thdmin", "Minimum distance for scoring");
    }

    public DependencyCalculator(Configuration config) {
        super(config);
        th_d = config.getDoubleOption("thdmin");
        th_sim = config.getDoubleOption("thSim");
        outlierTable = config.getStringOption("outlierTable");
        timeColumn = config.getStringOption("timeColumn");
        subgraphName = config.getStringOption("subgraphName");
        subgraphTable = config.getStringOption("subgraphTable");
        inputTable = config.getStringOption("inputTable");
        graphTable = config.getStringOption("graphTable");


    }


    public List<DependencyRecord> run(List<Subgraph> spmGraphs) {
        //parse subgraphs
        Map<Integer, List<Subgraph>> streetIdToSubgraph = parseSubGraphs(spmGraphs);
        Map<Subgraph, Integer> subgraphToIndex = enumerateSubgraphs(spmGraphs);

        int noSubgraphs = streetIdToSubgraph.keySet().size();
        int numberOfTimePoints = getNumberOfTimePoints();

        int[][] occurenceMatrix = new int[noSubgraphs][numberOfTimePoints];
        Set<Pair<Subgraph, Subgraph>> candidates = populateOccurceMatrix(occurenceMatrix, streetIdToSubgraph, subgraphToIndex);

        //calculate pairwise dependency

        List<DependencyRecord> dependencyScores = Collections.synchronizedList(new ArrayList<>());

        ProgressBar pb = new ProgressBar("Computing Scores", candidates.size());
        pb.start();

        PostgreDB db = new PostgreDB(config);
        candidates.stream().parallel().forEach(cand -> {
            Subgraph left = cand.getLeft();
            Subgraph right = cand.getRight();

            double distance=calculateDistance(left, right, db);

            if (distance >= th_d) {
                int leftIndex = subgraphToIndex.get(cand.getLeft());
                int rightIndex = subgraphToIndex.get(cand.getRight());
               double mi =  MutualInformation.mutualInformation(occurenceMatrix[leftIndex], occurenceMatrix[rightIndex]);
                double score = mi / distance;
               dependencyScores.add(new DependencyRecord(cand.getLeft().getId(),
                        cand.getRight().getId(),
                        distance,
                        mi,
                        score));
            }
            pb.step();
        });
        pb.stop();
        return dependencyScores;
    }




    private  Map<Integer, List<Subgraph>>  parseSubGraphs(List<Subgraph> spmGraphs) {
        Map<Integer, List<Subgraph>> streetIdToSubgraph = new HashMap<>();

        spmGraphs.stream().forEach(sg -> {
            for (LabeledDefaultEdge e: sg) {
                if (!streetIdToSubgraph.containsKey(e.label)) {
                    streetIdToSubgraph.put(e.label, new ArrayList<>());
                }
                streetIdToSubgraph.get(e.label).add(sg);
            }
        });
        return  streetIdToSubgraph;
    }

    private Map<Subgraph, Integer> enumerateSubgraphs(List<Subgraph> spmGraphs) {
        Map<Subgraph, Integer> result = new HashMap<>();

        for(int i=0; i<spmGraphs.size(); ++i) {
            result.put(spmGraphs.get(i), i);
        }
        return result;
    }



    private int getNumberOfTimePoints() {
        int result=-1;

        PostgreDB db = new PostgreDB(config);
        String query = "select count(distinct "+timeColumn+")" +
                " from "+outlierTable+";";

        try(Connection con= db.getConnection();
            Statement stmt=con.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                result = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(3);
        }
        return result;
    }

    private Set<Pair<Subgraph, Subgraph>> populateOccurceMatrix(int[][] occurenceMatrix,
                                                                Map<Integer, List<Subgraph>> streetIdToSubgraph,
                                                                Map<Subgraph, Integer> subgraphToIndex) {
        Set<Pair<Subgraph, Subgraph>> candidates = new HashSet<>();

        String columnsHead = "select ot.id,"+timeColumn;

        String query =  " from "+outlierTable+" ot\n" +
                        "join "+subgraphTable+" sg on (ot.id = sg.id)\n" +
                        "where sg.name='"+subgraphName+"'\n";

        String orderFooter = "order by "+timeColumn+" asc;";


        PostgreDB db = new PostgreDB(config);
        try(Connection con= db.getConnection();
            Statement stmt=con.createStatement();
            ResultSet rs = stmt.executeQuery(columnsHead+query+orderFooter)) {


            long currentTime = -1;
            int currentTimeIndex = -1;
            Set<Subgraph> currentAffected = new HashSet<>();

            while(rs.next()) {
                long t = rs.getTimestamp(2).getTime();
                int streetId = rs.getInt(1);


                //reached a new time point
                if (t != currentTime) {
                    //increment time
                    ++currentTimeIndex;
                    currentTime = t;
                    //save candidates
                    addPairsToCandidateSet(candidates, currentAffected);
                    currentAffected = new HashSet<>();
                }

                //ignore isolated outliers
                if (! streetIdToSubgraph.containsKey(streetId)) {
                    continue;
                }

                for (Subgraph sg: streetIdToSubgraph.get(streetId)) {
                    currentAffected.add(sg);
                    int subgraphIndex = subgraphToIndex.get(sg);
                    occurenceMatrix[subgraphIndex][currentTimeIndex]=1;
                }
            }
            //save last candidates
            addPairsToCandidateSet(candidates, currentAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
        return candidates;
    }

    private void addPairsToCandidateSet(Set<Pair<Subgraph, Subgraph>> candidateSet, Set<Subgraph> currentStep) {
        List<Subgraph> current = new ArrayList<>(currentStep);

        for (int i=0; i<current.size(); ++i) {
            Subgraph first = current.get(i);
            for (int j=i+1; j < current.size(); ++j) {
                Subgraph second = current.get(j);
                Pair<Subgraph, Subgraph> candidate = new ImmutablePair<>(first, second);
                Pair<Subgraph, Subgraph> candidateReversed = new ImmutablePair<>(second, first);
                if ((!candidateSet.contains(candidate)) && (!candidateSet.contains(candidateReversed))) {
                    candidateSet.add(candidate);
                }
            }
        }
    }

    private double calculateDistance(Subgraph left, Subgraph right, PostgreDB db) {
        String query = "select distances.c1, distances.c2, min(distances.dist)\n" +
                "from\n" +
                "    (select c1.subgraph c1, c2.subgraph c2,\n" +
                "    public.st_distance(sg1.geometry::public.geography, sg2.geometry::public.geography) dist\n" +
                "    from\n" +
                "    st_subgraphs c1\n" +
                "    join "+graphTable+" sg1 on (c1.street = sg1.id),\n" +
                "    st_subgraphs c2\n" +
                "    join "+graphTable+" sg2 on (c2.street = sg2.id)\n" +
                "    where c1.subgraph=? and c2.subgraph=?" +
                "        and c1.config=?" +
                "        and c2.config=?) distances\n" +
                "group by distances.c1, distances.c2;";

        double result=-1;
        try(Connection con = db.getConnection();
            PreparedStatement stmt = con.prepareStatement(query);)
        {

            stmt.setInt(1,  left.getId());
            stmt.setInt(2,  right.getId());
            stmt.setInt(3, config.getIntVariable("id"));
            stmt.setInt(4, config.getIntVariable("id"));


            try(ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result = rs.getDouble(3);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(5);
        }
        return result;
    }

}
