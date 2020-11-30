package org.opencb.bionetdb.core.models.network;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 10/08/15.
 */
public class Network extends Graph {

    private String id;
    private String name;
    private String description;

    private Map<String, Object> attributes;

    private long numNodes;
    private long numRelations;

    public Network() {
        this("", "", "");
    }

    public Network(String id, String name, String description) {
        super();
        this.id = id;
        this.name = name;
        this.description = description;

        attributes = new HashMap<>();
    }

    public void write(File file) throws FileNotFoundException, JsonProcessingException {
        Network network = new Network(id, name, description);
        network.setAttributes(attributes);
        network.setNumNodes(nodes.size());
        network.setNumRelations(relations.size());

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        PrintWriter pw = new PrintWriter(file);

        pw.println(mapper.writer().writeValueAsString(network));
        for (Node node: nodes) {
            pw.println(mapper.writer().writeValueAsString(node));
        }
        for (Relation relation: relations) {
            pw.println(mapper.writer().writeValueAsString(relation));
        }

        pw.close();


    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Network{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", nodes=").append(nodes);
        sb.append(", relations=").append(relations);
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Network setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Network setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Network setDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Network setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Network setNumNodes(long numNodes) {
        this.numNodes = numNodes;
        return this;
    }

    public Network setNumRelations(long numRelations) {
        this.numRelations = numRelations;
        return this;
    }
}
