package org.opencb.bionetdb.core.neo4j;

import junit.framework.Assert;
import org.junit.Test;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.ExpressionParser;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertNotSame;
import static junit.framework.TestCase.assertEquals;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Created by imedina on 21/08/15.
 */
public class Neo4JNetworkDBAdaptorTest {

    @Test
    public void testInsert() throws Exception {
        String database = "/tmp/neodb";
        new File(database).mkdirs();
        NetworkDBAdaptor networkDBAdaptor = null;
        long startTime = System.currentTimeMillis();

        try {
            System.out.println("Parsing network...");
            BioPaxParser bioPaxParser = new BioPaxParser("L3");
            Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
            //Path inputPath = Paths.get("/Users/pfurio/Downloads/biopax/Homo_sapiens.owl.gz");
            Network network = bioPaxParser.parse(inputPath);
            long stopTime = System.currentTimeMillis();
            System.out.println("Parsed in " + (stopTime-startTime)/1000 + " seconds");

            System.out.println("Creating database...");
            networkDBAdaptor = new Neo4JNetworkDBAdaptor(database);
            startTime = System.currentTimeMillis();
            System.out.println("Database created in " + (startTime - stopTime) / 1000 + " seconds");

            startTime = System.currentTimeMillis();
            System.out.println("Inserting data...");
            networkDBAdaptor.insert(network, null);
            stopTime = System.currentTimeMillis();
            System.out.println("Data inserted in " + (stopTime - startTime)/1000 + " seconds");

            // Nodes + relationships
            String firstInsert = networkDBAdaptor.stats(null, null).getId();

            startTime = System.currentTimeMillis();
            System.out.println("Inserting the same data...");
            networkDBAdaptor.insert(network, null);
            stopTime = System.currentTimeMillis();
            System.out.println("Data ¿reinserted? in " + (stopTime - startTime)/1000 + " seconds");

            // Nodes + relationships
            String secondInsert = networkDBAdaptor.stats(null, null).getId();
            assertEquals("Insert both times the same network", firstInsert, secondInsert);

            System.out.println("Annotating Xrefs via physical entity ID...");

            List<Xref> mylist = new ArrayList<Xref>();
            for (int i = 0; i < 4; i++) {
                mylist.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
            }

            String query = "MATCH (z:Xref) WHERE z.id = 'id3' RETURN count(z)";
            networkDBAdaptor.addXrefs("Protein1686", mylist);
            QueryOptions query_options = new QueryOptions();
            query_options.put("columnsAs", "count(z)");
            String myresult = networkDBAdaptor.get(new Query("query", query), query_options).getId();
            assertEquals("Annotation of Xref to one protein", "1", myresult);

            System.out.println("Annotating Xrefs via Node ID...");
            mylist.clear();
            for (int i = 3; i < 8; i++) {
                mylist.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
            }
            networkDBAdaptor.addXrefs("id2", mylist);
            myresult = networkDBAdaptor.get(new Query("query", query), query_options).getId();
            assertEquals("Annotation of the same node via an Xref ID", "1", myresult);

            System.out.println(networkDBAdaptor.stats(null, null).getId());

            System.out.println("\n\nTesting the insertion of expression data");
            System.out.println("========================================\n\n");

            Path metadata = Paths.get(getClass().getResource("/expression/myfactors.txt").toURI());
            ExpressionParser expressionParser = new ExpressionParser(metadata);

            System.out.println("Inserting new expression data...");
            Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
            for (String tissue : allExpressionFiles.keySet()) {
                for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                    List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                    networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, false);
                }
            }
            String expressStats = networkDBAdaptor.stats(null, null).getId();
            System.out.println(expressStats);

            System.out.println("Reinserting same expression data...");
            for (String tissue : allExpressionFiles.keySet()) {
                for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                    List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                    networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, false);
                }
            }

            assertEquals("The number of nodes/relations has changed after reinserting the same expression info",
                    expressStats, networkDBAdaptor.stats(null, null).getId());

            System.out.println("Reinserting same expression data, but wanting ids not found in the database to be created together with the expression info");
            for (String tissue : allExpressionFiles.keySet()) {
                for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                    List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                    networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, true);
                }
            }

            assertNotSame("Expression data from nodes not found in the database have not created new nodes/relations",
                    expressStats, networkDBAdaptor.stats(null, null).getId());
            System.out.println(networkDBAdaptor.stats(null, null).getId());

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