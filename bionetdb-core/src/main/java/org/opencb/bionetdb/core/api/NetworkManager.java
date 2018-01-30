package org.opencb.bionetdb.core.api;

import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.Network;
import org.opencb.bionetdb.core.models.Node;
import org.opencb.bionetdb.core.models.Relation;

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

        // Relation support
        relationshipsIndex = new HashMap<>();
        relationshipsUids = new HashMap<>();
        size = network.getRelations().size();
        for (int i = 0; i < size; i++) {
            Relation relation = network.getRelations().get(i);
            relationshipsIndex.put(relation.getUid(), i);
            if (!relationshipsUids.containsKey(relation.getId())) {
                relationshipsUids.put(relation.getId(), new ArrayList<>());
            }
            relationshipsUids.get(relation.getId()).add(relation.getUid());
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

    public Relation getRelationship(int uid) {
        return network.getRelations().get(relationshipsIndex.get(uid));
    }

    public List<Relation> getRelationships(String id) {
        List<Relation> relations = new ArrayList<>();
        for (int uid: relationshipsUids.get(id)) {
            relations.add(getRelationship(uid));
        }
        return relations;
    }

    public void setRelationship(Relation relation) throws BioNetDBException {
        if (relation != null) {
            if (!relationshipsIndex.containsKey(relation.getUid())) {
                network.getRelations().add(relation);
                relationshipsIndex.put(relation.getUid(), network.getRelations().indexOf(relation));
                if (!relationshipsUids.containsKey(relation.getId())) {
                    relationshipsUids.put(relation.getId(), new ArrayList<>());
                }
                relationshipsUids.get(relation.getId()).add(relation.getUid());
            } else {
                throw new BioNetDBException("Relation UID '" + relation.getUid() + "' is not unique");
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
