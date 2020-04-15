package de.l3s.st_discovery;

import de.l3s.st_discovery.graph.GraphFetcher;
import de.l3s.st_discovery.graph.Subgraph;
import de.l3s.st_discovery.model.DependencyCalculator;
import de.l3s.st_discovery.model.OutlierIdentifier;
import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;
import de.l3s.st_discovery.model.regiongrowing.RegionGrowing;
import de.l3s.st_discovery.model.SpatialMerging;
import de.l3s.st_discovery.model.regiongrowing.Segment;
import de.l3s.st_discovery.output.DependencyRecord;
import de.l3s.st_discovery.output.DependencyRecordWriter;
import de.l3s.st_discovery.output.SubgraphWriter;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import de.l3s.st_discovery.util.db.PostgreDB;

import org.jgrapht.Graph;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class Application {

    private List<Configuration> configs;

    public Application(String[] args) {
        configs = parseConfig(args);


    }

    private List<Configuration> parseConfig(String[] args) {
        ConfigurationParser confParse = new ConfigurationParser();

        PostgreDB.addConfigEntriesWitMaxCons(confParse);
        OutlierIdentifier.registerConfigEntries(confParse);
        GraphFetcher.registerConfigEntries(confParse);
        RegionGrowing.registerConfigEntries(confParse);
        SpatialMerging.registerConfigEntries(confParse);
        DependencyCalculator.registerConfigEntries(confParse);

        confParse.addBooleanOption("oi", "outlierIdentification", "If provided outlier will be computed");

        confParse.parse(args);
        return confParse.getConfigs();
    }

    private void run() throws SQLException {
        for (Configuration c: configs) {
            runConfig(c);
        }
    }

    private void runConfig(Configuration config) throws SQLException {
        determineConfigID(config);

        //outlier identification
        boolean doOutlierIdentification = config.getBooleanOption("outlierIdentification", false);
        if (doOutlierIdentification) {
            OutlierIdentifier oi = new OutlierIdentifier(config);
            oi.run();
        }

        //fetch street network data from db
        GraphFetcher gf = new GraphFetcher(config);
        Graph<Integer, LabeledDefaultEdge> graph = gf.fetchGraph();
        SortedMap<Date, Set<Segment>> outliers = gf.fetchOutlierMap();

        //identifaction of affected subgraphs
        RegionGrowing rg  = new RegionGrowing(config, graph, outliers, gf.getEdgesMap(), gf.getMaxNodeId());
        Map<Date, List<Subgraph>> rgGraphs = rg.run();

        //spatial merging
        SpatialMerging spm = new SpatialMerging(config);
        List<Subgraph> spmGraphs = spm.run(rgGraphs);

        //write subgraphs
        SubgraphWriter subgraphWriter = new SubgraphWriter(config);
        subgraphWriter.writeSubgraphsToDB(spmGraphs);

        //strucutural dependencies
        DependencyCalculator dependencyCalculator = new DependencyCalculator(config);
        List<DependencyRecord> result = dependencyCalculator.run(spmGraphs);

        //writeResult
        DependencyRecordWriter dependencyRecordWriter = new DependencyRecordWriter(config);
        dependencyRecordWriter.writeToDB(result);

    }

    private void determineConfigID(Configuration config) {
        String inputTable = config.getStringOption("inputTable");
        String outlierTable = config.getStringOption("outlierTable");
        String idColumn = config.getStringOption("idColumn");
        String speedColumn =  config.getStringOption("speedColumn");
        String timeColumn  = config.getStringOption("timeColumn");
        String graphTable = config.getStringOption("graphTable");
        String subgraphTable = config.getStringOption("subgraphTable");
        String subgraphName = config.getStringOption("subgraphName");
        double thSim = config.getDoubleOption("thSim");
        double thdmin = config.getDoubleOption("thdmin");
        int dskip = config.getIntOption("dSkip", 2);

        String createQuery = "create table if not exists config(\n" +
                "    id serial primary key,\n" +
                "    input_table text,\n" +
                "    outlier_table text,\n" +
                "    idColumn text,\n" +
                "    speedColumn text,\n" +
                "    graphTable text,\n" +
                "    subgraphTable text,\n" +
                "    subgraphName text,\n" +
                "    th_sim double precision,\n" +
                "    th_dmin double precision,\n" +
                "    d_skip int,\n" +
                "    unique (input_table, outlier_table, idColumn, speedColumn," +
                " graphTable, subgraphTable, subgraphName, th_sim, th_dmin, d_skip)\n" +
                ")";

        String selectQuery = "select id from config where (" +
                "input_table='"+inputTable+"' " +
                "and outlier_table='"+outlierTable+"' " +
                "and idColumn='"+idColumn+"' " +
                "and speedColumn='"+speedColumn+"' " +
                "and graphTable='"+graphTable+"' " +
                "and subgraphTable='"+subgraphTable+"' " +
                "and subgraphName='"+subgraphName+"' " +
                "and th_sim="+thSim+" " +
                "and th_dmin="+thdmin+" " +
                "and d_skip="+dskip+") ";

        String insertQuery = "insert into config (input_table, " +
                "outlier_table," +
                "idColumn, " +
                "speedColumn, " +
                "graphTable, " +
                "subgraphTable, " +
                "subgraphName, " +
                "th_sim, " +
                "th_dmin, " +
                "d_skip) VALUES ("
                +"'"+inputTable+"', "
                +"'"+outlierTable+"', "
                +"'"+idColumn+"', "
                +"'"+speedColumn+"', "
                +"'"+graphTable+"', "
                +"'"+subgraphTable+"', "
                +"'"+subgraphName+"', "
                +thSim+", "
                +thdmin+", "
                +dskip+") RETURNING id";

        PostgreDB db = new PostgreDB(config);
        try (Connection con = db.getConnection();
             Statement createStmt = con.createStatement();
             Statement selectStmt = con.createStatement();
             Statement insrtStmt = con.createStatement()) {

            createStmt.execute(createQuery);

            int configID = -1;
            try (ResultSet rs = selectStmt.executeQuery(selectQuery)) {
                while (rs.next()) {
                    configID = rs.getInt(1);
                }
            }

            if (configID == -1) {
                try (ResultSet rs = insrtStmt.executeQuery(insertQuery)) {
                    while (rs.next()) {
                        configID = rs.getInt(1);
                    }
                }
            }
            config.setVariable("id", configID);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(5);
        }


    }


    public static void main(String[] args) throws SQLException {
        Application app = new Application(args);
        app.run();
    }
}
