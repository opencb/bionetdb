package org.opencb.bionetdb.core.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.models.*;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
//import org.opencb.datastore.core.Query;
//import org.opencb.datastore.core.QueryOptions;
//import org.opencb.datastore.core.QueryResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor, Closeable {

    private GraphDatabaseService database;
    private boolean openedDB = false;

    public Neo4JNetworkDBAdaptor(String database) {
        this.database = new GraphDatabaseFactory().newEmbeddedDatabase( database );
        this.openedDB = true;
    }

    private static enum RelTypes implements RelationshipType
    {
        REACTANT,
        CONTROLLED,
        CONTROLLER
    }

    @Override
    public void insert(Network network, QueryOptions queryOptions) {

        //Map <String,Long> myNodeIds = new HashMap<String,Long>();
        try ( Transaction tx = this.database.beginTx() ) {
            for (PhysicalEntity p : network.getPhysicalEntities()) {
                // Insert all the Physical entity nodes
                Node mynode = this.database.createNode();
                mynode.setProperty("ID", p.getId());
                mynode.setProperty("Name", p.getName());
                mynode.setProperty("Description", p.getDescription());
            //    myNodeIds.put(p.getId(),mynode.getId());
            }

            for (Interaction i : network.getInteractions()) {

                // Insert all the Interation nodes
                if (i.getType() == Interaction.Type.REACTION) {
                    Node mynode = this.database.createNode();
                    mynode.setProperty("ID", i.getId());
                    mynode.setProperty("Name", i.getName());
                    mynode.setProperty("Description", i.getDescription());
                }
                /*
                switch (i.getType()) {
                    case REACTION:
                        // Left & right
                        Reaction r = (Reaction) i;
*/
                        // Left reactions
                       /* Map<String, Object> params = new HashMap<String, Object>();
                        ArrayList<Long> ids2search = null;
                        for (String myID : r.getReactants()) {
                            ids2search.add(myNodeIds.get(myID));
                        }
                        params.put("ids", ids2search);*/
                /*
                        Result result = this.database.execute("MATCH n WHERE n.id in {ids} return n", params);
                        Iterator<Node> myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            n.createRelationshipTo(mynode, RelTypes.REACTANT);
                        }

                        // Right reactions
                        params.clear();
                        params.put("ids", r.getProducts());
                        result = this.database.execute("MATCH n WHERE id(n) in {ids} return n", params);
                        myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            mynode.createRelationshipTo(n, RelTypes.REACTANT);
                        }

                        break;
                    case CATALYSIS:
                        Catalysis catalysis = (Catalysis) i;

                        // Left reactions (controllers)
                        params = new HashMap<String, Object>();
                        params.put( "ids", Arrays.asList(catalysis.getControllers()));
                        result = this.database.execute("MATCH n WHERE id(n) in {ids} return n", params);
                        myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            n.createRelationshipTo(mynode, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        params.clear();
                        params.put("ids", Arrays.asList(catalysis.getControlledProcesses()));
                        result = this.database.execute("MATCH n WHERE id(n) in {ids} return n", params);
                        myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            mynode.createRelationshipTo(n, RelTypes.CONTROLLED);
                        }

                        break;

                    case REGULATION:

                        Regulation regulation = (Regulation) i;

                        // Left reactions (controllers)
                        params = new HashMap<String, Object>();
                        params.put( "ids", Arrays.asList(regulation.getControllers()));
                        result = this.database.execute("MATCH n WHERE id(n) in {ids} return n", params);
                        myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            n.createRelationshipTo(mynode, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        params.clear();
                        params.put("ids", Arrays.asList(regulation.getControlledProcesses()));
                        result = this.database.execute("MATCH n WHERE id(n) in {ids} return n", params);
                        myiterator = result.columnAs("n");
                        while (myiterator.hasNext()) {
                            Node n = myiterator.next();
                            mynode.createRelationshipTo(n, RelTypes.CONTROLLED);
                        }

                        break;
                    default:
                        break;
                }*/
            }

            tx.success();
        }


    }

    @Override
    public void insert(List<Interaction> interactionList, QueryOptions queryOptions) {

    }

    @Override
    public QueryResult get(Query query, QueryOptions queryOptions) {

        Result result = this.database.execute( query.get("query").toString());
        //return new QueryResult("get", 0, 0, 0, null, null, Collections.singletonList(result));
        return new QueryResult(result.toString());

    }

    @Override
    public QueryResult getPhysicalEntities(Query query, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public QueryResult stats(Query query, QueryOptions queryOptions) {
        return null;
    }

    public boolean isOpened() throws IOException {
        if (this.openedDB == true)
            return true;
        else
            throw new IOException("Database is closed.");
    }

    @Override
    public void close() throws IOException {
        this.database.shutdown();
    }

}
