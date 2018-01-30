package org.opencb.bionetdb.core.api;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Node;
import org.opencb.bionetdb.core.models.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetworkManager {

    private Network network;

    // Network node support
    private Map<Integer, Integer> nodesIndex;
    private Map<String, List<Integer>> nodesUids;

    // Network relationship support
    private Map<Integer, Integer> relationshipsIndex;
    private Map<String, List<Integer>> relationshipsUids;

    public NetworkManager(Network network) {
        this.network = network;
        init();
    }

    private void init() {
        // Initialize Network Manager from network

        // Node support
        nodesIndex = new HashMap<>();
        nodesUids = new HashMap<>();
        int size = network.getNodes().size();
        for (int i = 0; i < size; i++) {
            Node node = network.getNodes().get(i);
            nodesIndex.put(node.getUid(), i);
            if (!nodesUids.containsKey(node.getId())) {
                nodesUids.put(node.getId(), new ArrayList<>());
            }
            nodesUids.get(node.getId()).add(node.getUid());
        }

        // Relationship support
        relationshipsIndex = new HashMap<>();
        relationshipsUids = new HashMap<>();
        size = network.getRelationships().size();
        for (int i = 0; i < size; i++) {
            Relationship relationship = network.getRelationships().get(i);
            relationshipsIndex.put(relationship.getUid(), i);
            if (!relationshipsUids.containsKey(relationship.getId())) {
                relationshipsUids.put(relationship.getId(), new ArrayList<>());
            }
            relationshipsUids.get(relationship.getId()).add(relationship.getUid());
        }
    }

    public Node getNode(int uid) {
        return network.getNodes().get(nodesIndex.get(uid));
    }

    public List<Node> getNodes(String id) {
        List<Node> nodes = new ArrayList<>();
        for (int uid: nodesUids.get(id)) {
            nodes.add(getNode(uid));
        }
        return nodes;
    }

    public void setNode(Node node) throws BioNetDBException {
        if (node != null) {
            if (!nodesIndex.containsKey(node.getUid())) {
                network.getNodes().add(node);
                nodesIndex.put(node.getUid(), network.getNodes().indexOf(node));
                if (!nodesUids.containsKey(node.getId())) {
                    nodesUids.put(node.getId(), new ArrayList<>());
                }
                nodesUids.get(node.getId()).add(node.getUid());
            } else {
                throw new BioNetDBException("Node UID '" + node.getUid() + "' is not unique");
            }
        }
    }

    public Relationship getRelationship(int uid) {
        return network.getRelationships().get(relationshipsIndex.get(uid));
    }

    public List<Relationship> getRelationships(String id) {
        List<Relationship> relationships = new ArrayList<>();
        for (int uid: relationshipsUids.get(id)) {
            relationships.add(getRelationship(uid));
        }
        return relationships;
    }

    public void setRelationship(Relationship relationship) throws BioNetDBException {
        if (relationship != null) {
            if (!relationshipsIndex.containsKey(relationship.getUid())) {
                network.getRelationships().add(relationship);
                relationshipsIndex.put(relationship.getUid(), network.getRelationships().indexOf(relationship));
                if (!relationshipsUids.containsKey(relationship.getId())) {
                    relationshipsUids.put(relationship.getId(), new ArrayList<>());
                }
                relationshipsUids.get(relationship.getId()).add(relationship.getUid());
            } else {
                throw new BioNetDBException("Relationship UID '" + relationship.getUid() + "' is not unique");
            }
        }
    }

    public Network getNetwork() {
        return network;
   }

   public void setNetwork(Network network) {
        this.network = network;
        init();
   }

//    void load(Path path) throws IOException, BioNetDBException;
//    void load(Path path, QueryOptions queryOptions) throws IOException, BioNetDBException;
//
//    QueryResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException;
//    NetworkIterator iterator(Query query, QueryOptions queryOptions);
//
//    void annotate();
//    void annotateGenes(Query query, QueryOptions queryOptions);
//    void annotateVariants(Query query, QueryOptions queryOptions);
//
//    QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException;
}
