package org.opencb.bionetdb.core.neo4j;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.TransactionFailureException;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.ExpressionParser;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import sun.nio.ch.Net;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Created by imedina on 21/08/15.
 */
public class Neo4JNetworkDBAdaptorTest {

    String database = "/tmp/neodb";
    NetworkDBAdaptor networkDBAdaptor = null;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void initialize () {
        // Remove existing database
        try {
            deleteRecursively( new File( database) );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        // Create again the path to the database
        new File(database).mkdirs();
        networkDBAdaptor = new Neo4JNetworkDBAdaptor(database);
    }

    @After
    public void close () throws Exception {
        networkDBAdaptor.close();
    }

    @Test
    public void testInsert() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        long startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        long stopTime = System.currentTimeMillis();
        System.out.println("Insertion of data took " + (stopTime - startTime) / 1000 + " seconds.");
        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes introduced in the database is not correct", 27893, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships introduced in the database is not correct", 40384, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

        startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        stopTime = System.currentTimeMillis();
        System.out.println("Trying to insert the same data took " + (stopTime - startTime)/1000 + " seconds.");
        myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes introduced in the database is not correct", 27893, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships introduced in the database is not correct", 40384, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
    }

    @Test
    public void testAddExpressionData() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        QueryOptions options = new QueryOptions("addNodes", false);

        networkDBAdaptor.insert(network, null);
        System.out.println("Data has been inserted in the database.");

        System.out.println("Starting test to add expression data...");
        Path metadata = Paths.get(getClass().getResource("/expression/myfactors.txt").toURI());
        ExpressionParser expressionParser = new ExpressionParser(metadata);
        Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
            }
        }
        System.out.println("Expression data has been inserted in the database.");
        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 27933, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships after inserting the expression data is not correct", 40424, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
            }
        }
        System.out.println("The same expression data has been tried to be inserted in the database.");
        myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 27933, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships after inserting the expression data is not correct", 40424, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

        options.put("addNodes", true);
        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, options);
            }
        }
        System.out.println("The same expression data allowing the annotation of nodes not in the database has been tried to be inserted in the database.");
        myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 27940, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships after inserting the expression data is not correct", 40430, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

    }

    @Test
    public void testAddXrefs() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        networkDBAdaptor.insert(network, null);
        System.out.println("Data has been inserted in the database.");

        System.out.println("Starting test of annotation of xref elements...");
        List<Xref> myList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            myList.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
        }
        networkDBAdaptor.addXrefs("CMK1", myList);
        System.out.println("New xrefs added to the database.");
        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 27897, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships after inserting the expression data is not correct", 40416, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));

        System.out.println("Creating new xrefs containing one that was already inserted...");
        myList.clear();
        for (int i = 3; i < 8; i++) {
            myList.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
        }
        networkDBAdaptor.addXrefs("id2", myList);
        System.out.println("New xrefs added to the database.");
        myResult = networkDBAdaptor.getSummaryStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 27901, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalNodes"));
        assertEquals("The number of relationships after inserting the expression data is not correct", 40448, (int) ((ObjectMap) myResult.getResult().get(0)).get("totalRelations"));
    }

    @Test
    public void testGetSummaryStats() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        networkDBAdaptor.insert(network, null);
        System.out.println("Data has been inserted in the database.");

        QueryResult myResult = networkDBAdaptor.getSummaryStats(null, null);
        System.out.println("Querying the database to retrieve the stats took " + (myResult.getDbTime()/1000.0) + " seconds");
        ObjectMap myStats = (ObjectMap) myResult.getResult().get(0);
        for (String key : myStats.keySet()) {
            System.out.println(key + ": " + myStats.get(key));
        }

        networkDBAdaptor.close();
        exception.expect(TransactionFailureException.class);
        networkDBAdaptor.getSummaryStats(null, null);
    }

    @Test
    public void testGet() throws Exception {

        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        networkDBAdaptor.insert(network, null);
        System.out.println("Data has been inserted in the database.");

        // TESTING QUERIES
        Query myQueryObject = new Query();
        List<String> myXrefIds = new ArrayList<>();
        myXrefIds.add("UniProt:P27466");
        myXrefIds.add("CMK1");
        myXrefIds.add("P27466");
        myXrefIds.add("MIH1");
        myXrefIds.add("CMK2");

        myQueryObject.put(NetworkDBAdaptor.NetworkQueryParams.PE_ID.key(), myXrefIds);
        myQueryObject.put(NetworkDBAdaptor.NetworkQueryParams._JUMPS.key(), 2);
        networkDBAdaptor.get(myQueryObject, null);
        System.out.println("The query took " + networkDBAdaptor.get(myQueryObject, null).getDbTime() + " seconds.");
    }
}