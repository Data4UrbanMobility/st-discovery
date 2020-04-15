package de.l3s.st_discovery.output;

import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.db.PostgreDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DependencyRecordWriter extends Configurable {



    public DependencyRecordWriter(Configuration config) {
        super(config);
    }

    public void writeToDB(List<DependencyRecord> records) {
        PostgreDB db = new PostgreDB(config);

        String createQuery =    "create table if not exists structural_dependencies (\n" +
                "    id serial primary key ,\n" +
                "    subgraph_1 integer,\n" +
                "    subgraph_2 integer,\n" +
                "    distance double precision,\n" +
                "    mutual_information double precision,\n" +
                "    score double precision,\n" +
                "    config int references config," +
                "    calculation_date  timestamp without time zone default now()"+
                ");";

        String deleteQuery = "delete from structural_dependencies where config="+config.getIntVariable("id")+";";

        String insertQuery =    "INSERT INTO structural_dependencies (" +
                                "subgraph_1, " +
                                "subgraph_2, " +
                                "distance, " +
                                "mutual_information, " +
                                "score, " +
                                "config) values (?, ?, ?, ?, ?, ?);";

        try(Connection con = db.getConnection();
            Statement createStmt = con.createStatement();
            Statement deleteStmt = con.createStatement();
            PreparedStatement insertStmt = con.prepareStatement(insertQuery)) {

            createStmt.execute(createQuery);

            deleteStmt.execute(deleteQuery);

            for (int i=0; i<records.size(); ++i) {
                DependencyRecord r = records.get(i);

                insertStmt.setInt(1, r.subgraph1);
                insertStmt.setInt(2, r.subgraph2);
                insertStmt.setDouble(3, r.distance);
                insertStmt.setDouble(4, r.mutualInformation);
                insertStmt.setDouble(5, r.score);
                insertStmt.setInt(6, config.getIntVariable("id"));
                insertStmt.addBatch();

                if (i % 1000 == 0) insertStmt.executeBatch();
            }

            insertStmt.executeBatch();



        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
