package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by imedina on 10/08/15.
 */
public class Interaction {

    protected String id;
    protected String name;
    protected String description;
    protected List<String> source;
    protected List<String> participants;
    protected List<String> processOf;
    protected List<String> participantOf;
    protected List<String> controlledBy;
    protected List<String> pathwayComponentOf;

    protected Type type;

    protected Map<String, Object> attributes;

    enum Type {
        REACTION    ("reaction"),
        CATALYSIS   ("catalysis"),
        REGULATION  ("regulation"),
        ASSEMBLY    ("assembly"),
        TRANSPORT   ("transport");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }

    public Interaction() {
        init();
    }

    public Interaction(String description, String id, String name, Type type) {
        this.description = description;
        this.id = id;
        this.name = name;
        this.type = type;

        // init rest of attributes
        init();
    }

    private void init() {
        this.attributes = new HashMap<>();
        this.source = new ArrayList<>();
        this.participants = new ArrayList<>();
        this.processOf = new ArrayList<>();
        this.participantOf = new ArrayList<>();
        this.controlledBy = new ArrayList<>();
        this.pathwayComponentOf = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public List<String> getSource() {
        return source;
    }

    public void setSource(List<String> source) {
        this.source = source;
    }

    public List<String> getParticipants() {
        return participants;
    }

    public void setParticipants(List<String> participants) {
        this.participants = participants;
    }

    public List<String> getProcessOf() {
        return processOf;
    }

    public void setProcessOf(List<String> processOf) {
        this.processOf = processOf;
    }

    public List<String> getParticipantOf() {
        return participantOf;
    }

    public void setParticipantOf(List<String> participantOf) {
        this.participantOf = participantOf;
    }

    public List<String> getControlledBy() {
        return controlledBy;
    }

    public void setControlledBy(List<String> controlledOf) {
        this.controlledBy = controlledOf;
    }

    public List<String> getPathwayComponentOf() {
        return pathwayComponentOf;
    }

    public void setPathwayComponentOf(List<String> pathwayComponentOf) {
        this.pathwayComponentOf = pathwayComponentOf;
    }
}
