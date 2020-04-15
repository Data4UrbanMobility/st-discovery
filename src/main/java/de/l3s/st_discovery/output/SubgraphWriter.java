package de.l3s.st_discovery.output;

import de.l3s.st_discovery.graph.Subgraph;
import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;
import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.db.PostgreDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SubgraphWriter extends Configurable {


    public SubgraphWriter(Configuration config) {
        super(config);
    }

    public void writeSubgraphsToDB(List<Subgraph> subgraphs) {
        PostgreDB db = new PostgreDB(config);

        String createQuery = "create table if not exists st_subgraphs (\n" +
                "        id serial primary key,\n" +
                "        subgraph integer,\n" +
                "        street integer,\n" +
                "        config integer references config,\n" +
                "        calculation_date timestamp without time zone default now()\n" +
                ");";

        String deleteQuery = "delete from st_subgraphs \n" +
                "where config="+config.getIntVariable("id")+";";

        String insertQuery = "INSERT INTO st_subgraphs (subgraph, street, config) " +
                "VALUES (?, ?, ?);";


        try(Connection con = db.getConnection();
            Statement createStmt = con.createStatement();
            Statement deleteStmt = con.createStatement();
            PreparedStatement insertStmt = con.prepareStatement(insertQuery)) {

            createStmt.execute(createQuery);
            deleteStmt.execute(deleteQuery);

            int counter = 0;
            for (Subgraph sg: subgraphs) {
                for(LabeledDefaultEdge edge: sg) {
                    insertStmt.setInt(1, sg.getId());
                    insertStmt.setInt(2, edge.label);
                    insertStmt.setInt(3, config.getIntVariable("id"));
                    insertStmt.addBatch();
                    if (++counter % 1000 == 0) {
                        insertStmt.executeBatch();
                    }
                }
            }
            insertStmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
