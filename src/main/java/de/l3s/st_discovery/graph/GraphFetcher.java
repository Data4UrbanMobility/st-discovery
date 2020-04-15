package de.l3s.st_discovery.graph;

import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;
import de.l3s.st_discovery.model.regiongrowing.Segment;
import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import de.l3s.st_discovery.util.db.PostgreDB;

import org.geotools.factory.Hints;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class GraphFetcher extends Configurable {

    private Map<Integer, LabeledDefaultEdge> edgesMap;
    private int maxNodeId;
    private String graphTable, subgraphTable, subgraphName, outlierTable;


    public static void registerConfigEntries(ConfigurationParser cp) {
        cp.addStringOption("gt", "graphTable", "table that holds the streetnetwork as graph");
        cp.addStringOption("st", "subgraphTable", "table that holds the name of the subgraph of the streetnetwork");
        cp.addStringOption("sn", "subgraphName", "name of the target subgraph");
    }

    public GraphFetcher(Configuration config) {
        super(config);
        maxNodeId=0;
        edgesMap = new HashMap<>();

        graphTable = config.getStringOption("graphTable");
        subgraphTable = config.getStringOption("subgraphTable");
        subgraphName = config.getStringOption("subgraphName");
        outlierTable = config.getStringOption("outlierTable");
    }


    public Graph<Integer, LabeledDefaultEdge> fetchGraph() throws SQLException {
        Collection<Segment> segments = fetchSegmentsFromDB();

        Graph<Integer, LabeledDefaultEdge> g = new DefaultDirectedGraph<>(LabeledDefaultEdge.class);
        for (Segment segment : segments) {
            g.addVertex(segment.sourceNodeId);
            g.addVertex(segment.targetNodeId);
            LabeledDefaultEdge edge = g.addEdge(segment.sourceNodeId, segment.targetNodeId);
            if(edge != null){
                edge.label = segment.id;
                edge.node_source = segment.sourceNodeId;
                edge.node_sink = segment.targetNodeId;
                edgesMap.put(segment.id, edge);
                maxNodeId = (maxNodeId < edge.node_source) ? edge.node_source : maxNodeId;
                maxNodeId = (maxNodeId < edge.node_sink) ? edge.node_sink : maxNodeId;
                edge.setGeometry(segment.geom);
            }
        }
        return g;
    }

    private Collection<Segment> fetchSegmentsFromDB() throws SQLException {

        PostgreDB db = new PostgreDB(config);

        Connection con = db.getConnection();

        Statement fetchStmt = con.createStatement();
        String fetchQuery =  "select sg.id, public.st_astext(geometry), source, target  from "+graphTable+" sg " +
                "        join "+subgraphTable+" sub on (sg.id = sub.id) " +
                "        where name='"+subgraphName+"';";

        ResultSet rs = fetchStmt.executeQuery(fetchQuery);

        List<Segment> segments = new ArrayList<>();

        Hints hints = new Hints(Hints.CRS, DefaultGeographicCRS.WGS84);
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(hints);
        WKTReader2 reader = new WKTReader2(geometryFactory);


        while (rs.next()) {
            int id = rs.getInt(1);
            int source = rs.getInt(3);
            int target = rs.getInt(4);

            try {
                Geometry geom = reader.read(rs.getString(2));
                Segment segment = new Segment(id, source, target,  -1, -1, geom);
                segments.add(segment);
            } catch (ParseException e) {
                e.printStackTrace();
                System.exit(4);
            }


        }
        rs.close();
        fetchStmt.close();
        con.close();
        db.close();

        return segments;
    }

    public SortedMap<Date, Set<Segment>> fetchOutlierMap() throws SQLException {
        SortedMap<Date, Set<Segment>> result = new TreeMap<>();

        String selectOutlierQuery = "select o.id, o.time from "+outlierTable+" o " +
                "join "+subgraphTable+" sub on (o.id = sub.id) " +
                "where sub.name='"+subgraphName+"';";

        PostgreDB db = new PostgreDB(config);
        Connection con = db.getConnection();
        Statement selectOutlierStmt = con.createStatement();

        ResultSet rs = selectOutlierStmt.executeQuery(selectOutlierQuery);

        while (rs.next()) {
            int id = rs.getInt(1);
            Date t = new Date(rs.getTimestamp(2).getTime());

            if (!result.containsKey(t)) {
                result.put(t, new HashSet<>());
            }

            Segment s = new Segment(id);
            s.isOutlier = true;
            result.get(t).add(s);
        }

        rs.close();
        selectOutlierStmt.close();
        con.close();
        db.close();

        return result;
    }

    public Map<Integer, LabeledDefaultEdge> getEdgesMap() {
        return edgesMap;
    }

    public int getMaxNodeId() {
        return maxNodeId;
    }
}
