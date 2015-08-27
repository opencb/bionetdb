package org.opencb.bionetdb.core.neo4j;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryResult;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Created by imedina on 21/08/15.
 */
public class Neo4JNetworkDBAdaptorTest {

    @Test
    public void testInsert() throws Exception {
        String database = "/Users/pfurio/Downloads/neodb";
        NetworkDBAdaptor networkDBAdaptor = null;
        try {
            System.out.println("Parsing network...");
            BioPaxParser bioPaxParser = new BioPaxParser("L3");
            Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
            Network network = bioPaxParser.parse(inputPath);

            System.out.println("Creating database...");
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database);

            System.out.println("Inserting data...");
            networkDBAdaptor.insert(network, null);
            System.out.println(networkDBAdaptor.get(new Query("query", "MATCH (x) -[y:XREF]-> (z) RETURN x.id, z.id"), null));

            System.out.println("Annotating Xrefs via physical entity ID...");
            //System.out.println("XREFS:" + networkDBAdaptor.getXrefs("Protein1686").toString());

            List<Xref> mylist = new ArrayList<Xref>();
            for (int i = 0; i < 4; i++) {
                mylist.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
            }

            networkDBAdaptor.addXrefs("Protein1686", mylist);
            System.out.println(networkDBAdaptor.get(new Query("query", "MATCH (x) -[y:XREF]-> (z) RETURN x.id, z.id"), null));


            System.out.println("Annotating Xrefs via XREF ID...");
            mylist.clear();
            for (int i = 4; i < 8; i++) {
                mylist.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
            }
            networkDBAdaptor.addXrefs("id2", mylist);
            System.out.println(networkDBAdaptor.get(new Query("query", "MATCH (x) -[y:XREF]-> (z) RETURN x.id, z.id"), null));

        }
        finally {
            System.out.println("Closing and deleting database...");
            networkDBAdaptor.close();

            try
            {
                deleteRecursively( new File( database) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

        }

    }

}