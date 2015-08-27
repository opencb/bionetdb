package org.opencb.bionetdb.core.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.Schema;
import org.opencb.bionetdb.core.api.NetworkDBAdaptor;
import org.opencb.bionetdb.core.models.*;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.neo4j.io.fs.FileUtils.deleteRecursively;

/**
 * Created by imedina on 05/08/15.
 */
public class Neo4JNetworkDBAdaptor implements NetworkDBAdaptor {

    private static String DB_PATH;
    private GraphDatabaseService database;
    private boolean openedDB = false;

    public Neo4JNetworkDBAdaptor(String database) {
        this.DB_PATH = database;
        //this.database = new GraphDatabaseFactory().newEmbeddedDatabase( this.DB_PATH );
        this.database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( this.DB_PATH ).
                setConfig( GraphDatabaseSettings.node_keys_indexable, "id" ).
                //setConfig( GraphDatabaseSettings.relationship_keys_indexable, "ID" ).
                setConfig( GraphDatabaseSettings.node_auto_indexing, "true" ).
                //setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" ).
                newGraphDatabase();
        this.openedDB = true;
/*
        // Creation of the indexes
        try ( Transaction tx = this.database.beginTx() ) {
            Schema schema = this.database.schema();
            for (PhysicalEntity.Type mytype : PhysicalEntity.Type.values()) {
                schema.indexFor(DynamicLabel.label(mytype.toString()))
                        .on("ID")
                        .create();
            }

            for (Interaction.Type mytype : Interaction.Type.values()) {
                schema.indexFor(DynamicLabel.label(mytype.toString()))
                        .on("ID")
                        .create();
            }

            // Include the two other types of Reaction manually
            schema.indexFor(DynamicLabel.label("ASSEMBLY"))
                    .on("ID")
                    .create();
            schema.indexFor(DynamicLabel.label("TRANSPORT"))
                    .on("ID")
                    .create();


            tx.success();
        }
*/
    }

    private enum RelTypes implements RelationshipType
    {
        REACTANT,
        XREF,
        CONTROLLED,
        CONTROLLER
    }

    @Override
    public void insert(Network network, QueryOptions queryOptions) {

        this.insertPhysicalEntities(network.getPhysicalEntities(), queryOptions);
        this.insertInteractions(network.getInteractions(), queryOptions);

    }

    @Override
    public void insertInteractions(List<Interaction> interactionList, QueryOptions queryOptions) {

        // 1. Insert all interactions as nodes
        try ( Transaction tx = this.database.beginTx() ) {

            Label nodeLabel = DynamicLabel.label("Interaction");

            for (Interaction i : interactionList) {
                // Insert all the Interation nodes
                /*if (i.getType() == Interaction.Type.REACTION) {
                    nodeLabel = DynamicLabel.label(((Reaction) i).getReactionType().toString());
                } else {
                    nodeLabel = DynamicLabel.label(i.getType().toString());
                }*/
                Node mynode = this.database.createNode(nodeLabel);

                if (i.getName() != null) mynode.setProperty("name", i.getName());
                if (i.getId() != null) mynode.setProperty("id", i.getId());
                if (i.getDescription() != null) mynode.setProperty("description", i.getDescription());

            }
            tx.success();
        }

        // 2. Insert the interactions
        try ( Transaction tx = this.database.beginTx() ) {
            Label interactionLabel = DynamicLabel.label("Interaction");
            Label pEntityLabel     = DynamicLabel.label("PhysicalEntity");
            Node r;
            for (Interaction i : interactionList) {

                switch(i.getType()) {
                    case REACTION:

                        // Left & right
                        Reaction myreaction = (Reaction) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        for (String myID : myreaction.getReactants()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            n.createRelationshipTo(r, RelTypes.REACTANT);
                        }

                        for (String myID : myreaction.getProducts()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            r.createRelationshipTo(n, RelTypes.REACTANT);
                        }

                        break;
                    case CATALYSIS:

                        Catalysis catalysis = (Catalysis) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : catalysis.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            n.createRelationshipTo(r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : catalysis.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            r.createRelationshipTo(n, RelTypes.CONTROLLED);
                        }

                        break;

                    case REGULATION:

                        Regulation regulation = (Regulation) i;
                        r = this.database.findNode(interactionLabel, "id", i.getId());

                        // Left reactions (controllers)
                        for (String myID : regulation.getControllers()) {
                            Node n = this.database.findNode(pEntityLabel, "id", myID);
                            n.createRelationshipTo(r, RelTypes.CONTROLLER);
                        }

                        // Right reactions (controlled)
                        for (String myID : regulation.getControlledProcesses()) {
                            Node n = this.database.findNode(interactionLabel, "id", myID);
                            r.createRelationshipTo(n, RelTypes.CONTROLLED);
                        }

                        break;

                    default:
                        break;
                }
            }
            tx.success();
        }
    }

    @Override
    public void insertPhysicalEntities(List<PhysicalEntity> physicalEntityList, QueryOptions queryOptions) {

        try ( Transaction tx = this.database.beginTx() ) {
            Label nodeLabel = DynamicLabel.label("PhysicalEntity");
            for (PhysicalEntity p : physicalEntityList) {
                // Insert all the Physical entity nodes
                Node mynode = this.database.createNode(nodeLabel);
                mynode.setProperty("id", p.getId());
                mynode.setProperty("name", p.getName());
                mynode.setProperty("description", p.getDescription());
            }

            tx.success();
        }
    }

    @Override
    public void addXrefs(String nodeID, List<Xref> xref_list) {

        Label xrefLabel    = DynamicLabel.label("Xref");
        Label pEntityLabel = DynamicLabel.label("PhysicalEntity");
        try ( Transaction tx = this.database.beginTx() ) {
            // 1. We look for the ID in physical entities directly
            Node n = this.database.findNode(pEntityLabel, "id", nodeID);

            // 2. Otherwise, we look in other xrefs
            if (n == null) {
                Node xrefNode = this.database.findNode(xrefLabel, "id", nodeID);
                if (xrefNode != null) {
                    //Look for the physical entity to which the xref is associated with
                    n = xrefNode.getSingleRelationship(RelTypes.XREF, Direction.INCOMING).getStartNode();
                }
            }

            for (Xref x : xref_list) {
                Node xref_node = this.database.createNode(xrefLabel);
                if (x.getDb() != null) xref_node.setProperty("db", x.getDb());
                if (x.getId() != null) xref_node.setProperty("id", x.getId());
                if (x.getDbVersion() != null) xref_node.setProperty("dbVersion", x.getDbVersion());
                if (x.getIdVersion() != null) xref_node.setProperty("idVersion", x.getIdVersion());
                n.createRelationshipTo(xref_node, RelTypes.XREF);
            }
            tx.success();
        }
    }
/*
    @Override
    public QueryResult getXrefs(String idNode) {
        String myresult = "";
        try ( Transaction tx = this.database.beginTx() ) {

            Result result = this.database.execute("MATCH (n {ID: '" + idNode + "'}) -[u]-> (m:Xref) return m.db");
            Iterator<String> myiterator = result.columnAs("m.db");
            while (myiterator.hasNext()) {
                String r = myiterator.next();
                myresult.concat(r.toString());
            }

            tx.success();
        }

        return new QueryResult(myresult);
    }
*/
    @Override
    public QueryResult get(Query query, QueryOptions queryOptions) {

        Result result = this.database.execute(query.get("query").toString());
        return new QueryResult(result.resultAsString());

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
    public void close() throws Exception {

        this.database.shutdown();

    }

}
