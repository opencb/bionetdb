package org.opencb.bionetdb.core.network;

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
}
