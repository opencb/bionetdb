package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 19/08/15.
 */
public class Catalysis extends Interaction {

    private List<String> controllers;
    private List<String> controlledProcesses;
    private List<String> cofactors;
    private String controlType;

    public Catalysis() {
        super("", "", "", Type.CATALYSIS);
        init();
    }

    public Catalysis(String id, String name, String description) {
        super(id, name, description, Type.CATALYSIS);
        init();
    }

    private void init() {
        this.controllers = new ArrayList<>();
        this.controlledProcesses = new ArrayList<>();
        this.cofactors = new ArrayList<>();
    }

    public List<String> getControllers() {

        return controllers;
    }

    public void setControllers(List<String> controllers) {
        this.controllers = controllers;
    }

    public List<String> getControlledProcesses() {
        return controlledProcesses;
    }

    public void setControlledProcesses(List<String> controlledProcesses) {
        this.controlledProcesses = controlledProcesses;
    }

    public List<String> getCofactors() {
        return cofactors;
    }

    public void setCofactors(List<String> cofactors) {
        this.cofactors = cofactors;
    }

    public String getControlType() {
        return controlType;
    }

    public void setControlType(String controlType) {
        this.controlType = controlType;
    }
}
