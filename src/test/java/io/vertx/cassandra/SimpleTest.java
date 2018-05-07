package io.vertx.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.Test;

public class SimpleTest {
//
//    @Test
//    public void makeSureDatastaxDriverWorks() {
//        Cluster cluster = null;
//        try {
//            cluster = Cluster.builder()
//                    .addContactPoint("127.0.0.1")
//                    .withPort(cassandraContainer.getMappedPort(9046))
//                    .build();
//            Session session = cluster.connect();
//
//            ResultSet rs = session.execute("select release_version from system.local");
//            Row row = rs.one();
//            System.out.println(row.getString("release_version"));
//        } finally {
//            if (cluster != null) cluster.close();
//        }
//    }
}
