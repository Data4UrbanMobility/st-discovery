package de.l3s.st_discovery.model.regiongrowing;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.l3s.st_discovery.graph.Subgraph;
import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import de.l3s.st_discovery.util.db.PostgreDB;
import de.l3s.st_discovery.util.misc.ProgressBar;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;

public class RegionGrowing extends Configurable {

    private Map<Integer, LabeledDefaultEdge> edgesMap;
    private int maxNodeId;
    private Graph<Integer, LabeledDefaultEdge> graph;
    private SortedMap<Date, Set<Segment>> outliersMap;
    private int dskip;
    private boolean cacheDistances, writeRegions;
    private String subGraphName;
    private String regionTableName;

    public static void registerConfigEntries(ConfigurationParser cp) {
        cp.addIntOption("ds", "dSkip", "Maximum edge distance that can be skipped during region growing");
        cp.addBooleanOption("cd", "cacheDistances", "Enable caching of distances within the graph");
        cp.addBooleanOption("wr", "writeRegions", "Store the result of region growing in the database");
        cp.addStringOption("rtn", "regionTableName", "Name of the table to store regions");
    }

    public RegionGrowing(Configuration config, Graph<Integer, LabeledDefaultEdge> graph,
                         SortedMap<Date, Set<Segment>> outliersMap,
                         Map<Integer, LabeledDefaultEdge> edgesMap,
                         int maxNodeId) {
        super(config);
        this.edgesMap = edgesMap;
        this.maxNodeId = maxNodeId;
        this.graph = graph;
        this.outliersMap = outliersMap;

        this.dskip = config.getIntOption("dSkip", 2);
        this.cacheDistances = config.getBooleanOption("cacheDistances", false);
        this.subGraphName = config.getStringOption("subgraphName");
        this.writeRegions = config.getBooleanOption("writeRegions", false);
        this.regionTableName = config.getStringOption("regionTableName", "region_growing");
    }

    public Map<Date, List<Subgraph>> run() {
        Map<Integer, Map<Integer, Short>> distances = getEdgeCountDistanceMatrix(graph);
        System.out.println("Loaded edge count distances");

        Set<Date> timePoints = outliersMap.keySet();

        Map<Date, List<Subgraph>> regions = new ConcurrentHashMap<>();
        ProgressBar pb = new ProgressBar("Performing region growing", timePoints.size());
        pb.start();
        timePoints.stream().parallel().forEach(t -> {

            Set<Segment> outliers = outliersMap.get(t);


            List<Subgraph> clusters = applyRegionGrowingForClusters(distances, dskip, 2, outliers);
            regions.put(t, clusters);
            pb.step();
        });
        pb.stop();

        if (writeRegions) {
            writeRegions(regions);
        }

        return regions;
    }


    private Map<Integer, Map<Integer, Short>> getEdgeCountDistanceMatrix(Graph<Integer, LabeledDefaultEdge> graph) {
        String fname = "edgeCountDistances_" + subGraphName + "_cache.csv";
        Map<Integer, Map<Integer, Short>> distances = new ConcurrentHashMap<>();

        if (cacheDistances) {
            File distancesFile = new File(fname);
            if (distancesFile.exists() && distancesFile.length() != 0) {
                //read distances from the file
                BufferedReader br;
                try {
                    br = new BufferedReader(new FileReader(distancesFile));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] fields = line.split(",");
                        int i = Integer.parseInt(fields[0]);
                        int j = Integer.parseInt(fields[1]);
                        short val = Short.parseShort(fields[2]);

                        if (!distances.containsKey(i)) {
                            distances.put(i, new ConcurrentHashMap<>());
                        }
                        distances.get(i).put(j, val);
                    }
                } catch (IOException ex) {
                    Logger.getLogger(RegionGrowing.class.getName()).log(Level.SEVERE, null, ex);
                }
                return distances;
            }
        }

        //calculate distances
        List<Integer> vertices = new ArrayList<>(graph.vertexSet());
        DijkstraShortestPath<Integer, LabeledDefaultEdge> dijkstraAlg = new DijkstraShortestPath<>(graph);
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < vertices.size(); i++) {
            Integer v = vertices.get(i);
            Map<Integer, Short> vertexDistances = new ConcurrentHashMap<>();
            distances.put(v, vertexDistances);

            ShortestPathAlgorithm.SingleSourcePaths<Integer, LabeledDefaultEdge> paths = dijkstraAlg.getPaths(v);
            for (int j = 0; j < vertices.size(); j++) {
                Integer u = vertices.get(j);
                Runnable runnable = () -> {
                    GraphPath<Integer, LabeledDefaultEdge> path = paths.getPath(u);
                    vertexDistances.put(u, (short) ((path != null) ? path.getEdgeList().size() : Short.MAX_VALUE));
                };
                executor.execute(runnable);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(2);
        }

        //save distances
        if (cacheDistances) {
            File distancesFile = new File(fname);

            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(distancesFile));

                for (Integer i : distances.keySet()) {
                    Map<Integer, Short> vertexDistances = distances.get(i);
                    for (Integer j : vertexDistances.keySet()) {
                        bw.write(i + "," + j + "," + vertexDistances.get(j) + "\n");
                    }
                }
                bw.close();
            } catch (IOException ex) {
                Logger.getLogger(RegionGrowing.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return distances;
    }


    private List<Subgraph> applyRegionGrowingForClusters(Map<Integer, Map<Integer, Short>> distances,
                                                         int tolerance,
                                                         int minItems,
                                                         Set<Segment> outliers) {

        List<Subgraph> clusters = new ArrayList<>();
        for (Segment outlier : outliers) {
            LabeledDefaultEdge edge = edgesMap.get(outlier.id);
            if (edge != null) {
                List<Subgraph> closeClusters = new ArrayList<>();
                for (int i = 0; i < clusters.size(); i++) {
                    Subgraph cluster = clusters.get(i);
                    for (LabeledDefaultEdge edge2 : cluster) {
                        if (distances.get(edge.node_sink).get(edge2.node_source) <= tolerance) {
                            closeClusters.add(cluster);
                            break;
                        }
                    }
                }
                Subgraph cluster = new Subgraph();
                cluster.addEdge(edge);
                clusters.add(cluster);
                for (Subgraph closeCluster : closeClusters) {
                    cluster.addAll(closeCluster);
                }
                clusters.removeAll(closeClusters);
            }
        }

        for (int i = clusters.size() - 1; i >= 0; i--) {
            Subgraph cluster = clusters.get(i);
            if (cluster.size() < minItems) {
                clusters.remove(i);
            }
        }
        return clusters;
    }

    private void writeRegions(Map<Date, List<Subgraph>> regions) {
        PostgreDB db = new PostgreDB(config);


        try {
            Connection con = db.getConnection();
            String createQuery = "CREATE TABLE IF NOT EXISTS " + regionTableName + " (\n" +
                    "    id serial primary key,\n" +
                    "    subgraph_id integer,\n" +
                    "    time timestamp  without time zone,\n" +
                    "    streetgraph_name text,\n" +
                    "    edges integer[],\n" +
                    "    creation_date timestamp without time zone default now()\n" +
                    ")";

            Statement createStmt = con.createStatement();
            createStmt.execute(createQuery);
            createStmt.close();

            String insertQuery = "INSERT INTO " + regionTableName + " (subgraph_id, time, streetgraph_name, edges) VALUES (?, ?, ?, ?)";
            PreparedStatement insertStmt = con.prepareStatement(insertQuery);

            for (Map.Entry<Date, List<Subgraph>> rg : regions.entrySet()) {
                Date time = rg.getKey();
                List<Subgraph> subgraphs = rg.getValue();
                for (Subgraph sg : subgraphs) {
                    insertStmt.setInt(1, sg.getId());
                    insertStmt.setTimestamp(2, new Timestamp(time.getTime()));
                    insertStmt.setString(3, subGraphName);

                    Integer[] edgesArray = new Integer[sg.getEdges().size()];
                    int i = 0;
                    for (LabeledDefaultEdge edge : sg.getEdges()) {
                        edgesArray[i++] = edge.label;
                    }

                    Array sqlArray = con.createArrayOf("INTEGER", edgesArray);
                    insertStmt.setArray(4, sqlArray);

                    insertStmt.addBatch();
                }

            }
            insertStmt.executeBatch();
            insertStmt.close();
            con.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
