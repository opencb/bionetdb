package org.opencb.bionetdb.core.neo4j;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.exceptions.DBException;
import org.opencb.bionetdb.core.io.BioPaxParser;
import org.opencb.bionetdb.core.io.ExpressionParser;
import org.opencb.bionetdb.core.models.Expression;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Xref;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

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
        QueryResult myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes introduced in the database is not correct", 12085, myResult.getResult().get(0));
        assertEquals("The number of relationships introduced in the database is not correct", 10970, myResult.getResult().get(1));

        startTime = System.currentTimeMillis();
        networkDBAdaptor.insert(network, null);
        stopTime = System.currentTimeMillis();
        System.out.println("Trying to insert the same data took " + (stopTime - startTime)/1000 + " seconds.");
        myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes introduced in the database is not correct", 12085, myResult.getResult().get(0));
        assertEquals("The number of relationships introduced in the database is not correct", 10970, myResult.getResult().get(1));
    }

    @Test
    public void testAddExpressionData() throws Exception {
        BioPaxParser bioPaxParser = new BioPaxParser("L3");
        Path inputPath = Paths.get(getClass().getResource("/Saccharomyces_cerevisiae.owl.gz").toURI());
        Network network = bioPaxParser.parse(inputPath);
        System.out.println("The file has been parsed.");

        networkDBAdaptor.insert(network, null);
        System.out.println("Data has been inserted in the database.");

        System.out.println("Starting test to add expression data...");
        Path metadata = Paths.get(getClass().getResource("/expression/myfactors.txt").toURI());
        ExpressionParser expressionParser = new ExpressionParser(metadata);
        Map<String, Map<String, String>> allExpressionFiles = expressionParser.getMyFiles();
        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, false);
            }
        }
        System.out.println("Expression data has been inserted in the database.");
        QueryResult myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 12090, myResult.getResult().get(0));
        assertEquals("The number of relationships after inserting the expression data is not correct", 10975, myResult.getResult().get(1));

        // TODO: Add asserts to check the number of nodes inserted
        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, false);
            }
        }
        System.out.println("The same expression data has been tried to be inserted in the database.");
        myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 12090, myResult.getResult().get(0));
        assertEquals("The number of relationships after inserting the expression data is not correct", 10975, myResult.getResult().get(1));

        for (String tissue : allExpressionFiles.keySet()) {
            for (String timeseries : allExpressionFiles.get(tissue).keySet()) {
                List<Expression> myExpression = expressionParser.parse(tissue, timeseries);
                networkDBAdaptor.addExpressionData(tissue, timeseries, myExpression, true);
            }
        }
        System.out.println("The same expression data allowing the annotation of nodes not in the database has been tried to be inserted in the database.");
        myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 12097, myResult.getResult().get(0));
        assertEquals("The number of relationships after inserting the expression data is not correct", 10981, myResult.getResult().get(1));

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
        networkDBAdaptor.addXrefs("Protein1686", myList);
        System.out.println("New xrefs added to the database.");
        QueryResult myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 12089, myResult.getResult().get(0));
        assertEquals("The number of relationships after inserting the expression data is not correct", 10974, myResult.getResult().get(1));

        System.out.println("Creating new xrefs containing one that was already inserted...");
        myList.clear();
        for (int i = 3; i < 8; i++) {
            myList.add(new Xref("db" + i, "dbVersion" + i, "id" + i, "idVersion" + i));
        }
        networkDBAdaptor.addXrefs("id2", myList);
        System.out.println("New xrefs added to the database.");
        myResult = networkDBAdaptor.getStats(null, null);
        assertEquals("The number of nodes after inserting the expression data is not correct", 12093, myResult.getResult().get(0));
        assertEquals("The number of relationships after inserting the expression data is not correct", 10978, myResult.getResult().get(1));
    }

}