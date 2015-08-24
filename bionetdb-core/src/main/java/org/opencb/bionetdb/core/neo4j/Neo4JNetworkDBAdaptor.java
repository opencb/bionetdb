package org.opencb.bionetdb.core.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.models.*;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

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

        // First we create the indexes
        try ( Transaction tx = this.database.beginTx() ) {
            Schema schema = this.database.schema();
            schema.indexFor(DynamicLabel.label("Node"))
                    .on("ID")
                    .create();
            tx.success();
        }

        Label nodeLabel = DynamicLabel.label( "Node" );

        try ( Transaction tx = this.database.beginTx() ) {
            for (PhysicalEntity p : network.getPhysicalEntities()) {
                // Insert all the Physical entity nodes
                Node mynode = this.database.createNode(nodeLabel);
                mynode.setProperty("ID", p.getId());
                mynode.setProperty("Name", p.getName());
                mynode.setProperty("Description", p.getDescription());
            }

            for (Interaction i : network.getInteractions()) {
                // Insert all the Interation nodes
                if (i.getType() == Interaction.Type.REACTION) {
                    Node mynode = this.database.createNode(nodeLabel);
                    mynode.setProperty("ID", i.getId());
                    mynode.setProperty("Name", i.getName());
                    mynode.setProperty("Description", i.getDescription());
                }
            }
            tx.success();
        }

        try ( Transaction tx = this.database.beginTx() ) {
            for (Interaction i : network.getInteractions()) {
                if (i.getType() == Interaction.Type.REACTION) {
                    // Left & right
                    Reaction myreaction = (Reaction) i;

                    Result result = this.database.execute("MATCH (n {ID: '" + i.getId() + "'}) return n");
                    Iterator<Node> myiterator = result.columnAs("n");
                    Node r = myiterator.next();

                    for (String myID : myreaction.getReactants()) {
                        result = this.database.execute("MATCH (n {ID: '" + myID + "'}) return n");
                        Iterator<Node> myiterator2 = result.columnAs("n");
                        Node n = myiterator2.next();
                        n.createRelationshipTo(r, RelTypes.REACTANT);
                    }

                    for (String myID : myreaction.getProducts()) {
                        result = this.database.execute("MATCH (n {ID: '" + myID + "'}) return n");
                        Iterator<Node> myiterator2 = result.columnAs("n");
                        Node n = myiterator2.next();
                        r.createRelationshipTo(n, RelTypes.REACTANT);
                    }
                }
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
