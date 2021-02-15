package org.opencb.bionetdb.core.models.network;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetworkManager {

    private Network network;

    // Network node support
    private Map<Long, Integer> nodesIndex;
    private Map<String, List<Long>> nodesUids;

    // Network relationship support
    private Map<Long, Integer> relationsIndex;
    private Map<String, List<Long>> relationsUids;

    public NetworkManager(Network network) {
        this.network = network;
        init();
    }

    private void init() {
        // Initialize Network Manager from network

        // Node support
        nodesIndex = new HashMap<>();
        nodesUids = new HashMap<>();
        long size = network.getNodes().size();
        for (int i = 0; i < size; i++) {
            Node node = network.getNodes().get(i);
            nodesIndex.put(node.getUid(), i);
            if (!nodesUids.containsKey(node.getId())) {
                nodesUids.put(node.getId(), new ArrayList<>());
            }
            nodesUids.get(node.getId()).add(node.getUid());
        }

        // Relation support
        relationsIndex = new HashMap<>();
        relationsUids = new HashMap<>();
        size = network.getRelations().size();
        for (int i = 0; i < size; i++) {
            Relation relation = network.getRelations().get(i);
            relationsIndex.put(relation.getUid(), i);
            if (!relationsUids.containsKey(relation.getName())) {
                relationsUids.put(relation.getName(), new ArrayList<>());
            }
            relationsUids.get(relation.getName()).add(relation.getUid());
        }
    }

    public Node getNode(long uid) {
        return network.getNodes().get(nodesIndex.get(uid));
    }

    public List<Node> getNodes() {
        return network.getNodes();
    }

    public List<Node> getNodes(String id) {
        List<Node> nodes = new ArrayList<>();
        for (long uid: nodesUids.get(id)) {
            nodes.add(getNode(uid));
        }
        return nodes;
    }

    public List<Node> getNodes(Node.Label label) {
        List<Node> nodes = new ArrayList<>();
        for (Node node: network.getNodes()) {
            if (CollectionUtils.isNotEmpty(node.getLabels())) {
                if (node.getLabels().contains(label)) {
                    nodes.add(node);
                }
            }
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

    public Relation getRelationship(long uid) {
        return network.getRelations().get(relationsIndex.get(uid));
    }

    public List<Relation> getRelationships() {
        return network.getRelations();
    }

    public List<Relation> getRelationships(String id) {
        List<Relation> relations = new ArrayList<>();
        for (long uid: relationsUids.get(id)) {
            relations.add(getRelationship(uid));
        }
        return relations;
    }

    public void setRelationship(Relation relation) throws BioNetDBException {
        if (relation != null) {
            if (!relationsIndex.containsKey(relation.getUid())) {
                network.getRelations().add(relation);
                relationsIndex.put(relation.getUid(), network.getRelations().indexOf(relation));
                if (!relationsUids.containsKey(relation.getName())) {
                    relationsUids.put(relation.getName(), new ArrayList<>());
                }
                relationsUids.get(relation.getName()).add(relation.getUid());
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

    public void replaceUid(long oldUid, long newUid) {
        for (Node node: network.getNodes()) {
            if (node.getUid() == oldUid) {
                node.setUid(newUid);
                break;
            }
        }

        for (Relation relation: network.getRelations()) {
            if (relation.getOrigUid() == oldUid) {
                relation.setOrigUid(newUid);
            }
            if (relation.getDestUid() == oldUid) {
                relation.setDestUid(newUid);
            }
        }
    }

    public void replaceRelationNodeUids(Map<Long, Long> old2NewUid) {
        for (Relation relation: network.getRelations()) {
            if (old2NewUid.containsKey(relation.getOrigUid())) {
                relation.setOrigUid(old2NewUid.get(relation.getOrigUid()));
            }
            if (old2NewUid.containsKey(relation.getDestUid())) {
                relation.setDestUid(old2NewUid.get(relation.getDestUid()));
            }
        }
    }

    //    void load(Path path) throws IOException, BioNetDBException;
//    void load(Path path, QueryOptions queryOptions) throws IOException, BioNetDBException;
//
//    QueryResult<Node> query(Query query, QueryOptions queryOptions) throws BioNetDBException;
//    NodeIterator iterator(Query query, QueryOptions queryOptions);
//
//    void annotate();
//    void annotateGenes(Query query, QueryOptions queryOptions);
//    void annotateVariants(Query query, QueryOptions queryOptions);
//
//    QueryResult getSummaryStats(Query query, QueryOptions queryOptions) throws BioNetDBException;
}
