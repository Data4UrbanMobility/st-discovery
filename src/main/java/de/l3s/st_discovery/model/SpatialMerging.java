package de.l3s.st_discovery.model;

import de.l3s.st_discovery.graph.Subgraph;
import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;
import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;

import de.l3s.st_discovery.util.misc.ProgressBar;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SpatialMerging extends Configurable {

    private boolean changed;

    private Map<LabeledDefaultEdge, Set<Subgraph>> edgeToMergedSubgraph;
    private List<Subgraph> currentSubgraphs;
    private double thSim;
    private boolean writeSubgraphs;

    public SpatialMerging(Configuration config) {
        super(config);
        changed = true;
        edgeToMergedSubgraph = new ConcurrentHashMap<>();
        currentSubgraphs = new ArrayList<>();
        thSim = config.getDoubleOption("thSim");
        writeSubgraphs = config.getBooleanOption("writeSubgraphs", false);
    }

    public static void registerConfigEntries(ConfigurationParser cp) {
        cp.addDoubleOption("ths", "thSim", "Similarity threshold for spatial merging");
        cp.addBooleanOption("wsg", "writeSubgraphs", "Write the result of spatial merging to a file");
    }

    public List<Subgraph> run(Map<Date, List<Subgraph>> rgGraphs) {
        currentSubgraphs = new ArrayList<>();
        parseRegionGrowingGraphs(rgGraphs);

        changed = true;
        int stepCounter = 1;
        while (changed) {
            System.out.println("Merging subgraphs iteration: " + stepCounter);
            changed = false;

            Set<Pair<Subgraph, Subgraph>> candidates = determineCandidates(currentSubgraphs);

            Map<Pair<Subgraph, Subgraph>, Double> scores = computeSimilarity(candidates);

            changed = mergeStep(scores);
            ++stepCounter;
        }

        if (writeSubgraphs) {
            writeCurrentSubgraphsToFile();
        }

        return currentSubgraphs;
    }


    private void parseRegionGrowingGraphs(Map<Date, List<Subgraph>> rgGraphs) {
        for (List<Subgraph> subgraphs : rgGraphs.values()) {
            currentSubgraphs.addAll(subgraphs);

            for (Subgraph sg : subgraphs) {
                for (LabeledDefaultEdge edge : sg) {
                    if (!edgeToMergedSubgraph.containsKey(edge)) {
                        edgeToMergedSubgraph.put(edge, new HashSet<>());
                    }
                    edgeToMergedSubgraph.get(edge).add(sg);
                }
            }
        }
    }


    private Set<Pair<Subgraph, Subgraph>> determineCandidates(List<Subgraph> graphs) {
        Set<Pair<Subgraph, Subgraph>> result = Collections.synchronizedSet(new HashSet<>());
        ProgressBar pb = new ProgressBar("Determining candidates", graphs.size());
        pb.start();
        //for (Subgraph sg: graphs) {
        graphs.stream().parallel().forEach(sg -> {
            Set<Subgraph> stepCandidates = new HashSet<>();

            for (LabeledDefaultEdge edge : sg) {
                stepCandidates.addAll(edgeToMergedSubgraph.get(edge));
                stepCandidates.remove(sg);
            }

            for (Subgraph candidate : stepCandidates) {
                Pair<Subgraph, Subgraph> current = new ImmutablePair<>(sg, candidate);
                Pair<Subgraph, Subgraph> reversed = new ImmutablePair<>(candidate, sg);

                if (!(result.contains(current) || result.contains(reversed))) {
                    result.add(current);
                }
            }
            pb.step();
        });
        pb.stop();
        return result;
    }

    private Map<Pair<Subgraph, Subgraph>, Double> computeSimilarity(Set<Pair<Subgraph, Subgraph>> candidates) {
        Map<Pair<Subgraph, Subgraph>, Double> result = new ConcurrentHashMap<>();

        ProgressBar pb = new ProgressBar("Computing similarites", candidates.size());
        pb.start();
        //for (Pair<Subgraph, Subgraph> current: candidates) {
        candidates.stream().parallel().forEach(current -> {
            Subgraph lhs = current.getLeft();
            Subgraph rhs = current.getRight();

            int shared = lhs.intersection(rhs).size();
            int total = lhs.union(rhs).size();

            double score = ((double) shared) / ((double) total);

            if (lhs.isSubsetOf(rhs) || rhs.isSubsetOf(lhs)) {
                score = 1.0;
            }

            if (score >= thSim) result.put(current, score);

            pb.step();
        });
        pb.stop();
        return result;
    }


    private boolean mergeStep(Map<Pair<Subgraph, Subgraph>, Double> scores) {
        Map<Pair<Subgraph, Subgraph>, Double> sortedCandiddates = scores.entrySet()
                .stream()
                .sorted(Map.Entry.<Pair<Subgraph, Subgraph>, Double>comparingByValue().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        AtomicBoolean mergeOccured = new AtomicBoolean(false);
        Set<Subgraph> visited = Collections.synchronizedSet(new HashSet<>());

        ProgressBar pb = new ProgressBar("Merging graphs", sortedCandiddates.keySet().size());
        pb.start();

        //for (Pair<Subgraph, Subgraph> current: sortedCandiddates.keySet()) {
        sortedCandiddates.keySet().stream().forEach(current -> {

            Subgraph lhs = current.getLeft();
            Subgraph rhs = current.getRight();
            Double score = sortedCandiddates.get(current);

            if (visited.contains(lhs) || visited.contains(rhs)) {
                pb.step();
                return;
            }

            if (score >= thSim) {
                mergeStep(lhs, rhs);
                mergeOccured.set(true);
                visited.add(lhs);
                visited.add(rhs);
            }
            pb.step();
        });
        pb.stop();
        return mergeOccured.get();
    }

    private void mergeStep(Subgraph lhs, Subgraph rhs) {
        lhs.addAll(rhs);

        for (LabeledDefaultEdge edge : rhs) {
            boolean result = edgeToMergedSubgraph.get(edge).remove(rhs);
            edgeToMergedSubgraph.get(edge).add(lhs);
        }

        currentSubgraphs.remove(rhs);
    }

    private void writeCurrentSubgraphsToFile() {
        String graphTable = config.getStringOption("graphTable");
        String outlierTable = config.getStringOption("outlierTable");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm");
        String fname = graphTable+"_"+outlierTable+"_"+thSim+"_"+dtf.format(LocalDateTime.now());

        String dirName = "subgraphs";
        File dir = new File(dirName);
        if (!dir.exists()) dir.mkdir();

        FileWriter fw = null;
        PrintWriter pw = null;
        try {
            fw = new FileWriter(dirName+"/"+fname);

            pw = new PrintWriter(fw);

            for (Subgraph sg : currentSubgraphs) {
                List<String> labels = new ArrayList<>();
                for (LabeledDefaultEdge lde : sg) {
                    labels.add(Integer.toString(lde.label));
                }
                pw.println(sg.getId()+"\t"+String.join(",", labels));
            }

            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (pw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
