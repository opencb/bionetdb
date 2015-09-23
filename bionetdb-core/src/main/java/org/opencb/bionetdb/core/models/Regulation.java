package org.opencb.bionetdb.core.models;

import java.util.*;

/**
 * Created by dapregi on 19/08/15.
 */
public class Regulation extends Interaction {

    private List<String> controllers;
    private List<String> controlledProcesses;
    private String controlType;

    public Regulation() {
        super("", "", new ArrayList<>(), Type.REGULATION);
        init();
    }

    public Regulation(String id, String name, List<String> description) {
        super(id, name, description, Type.REGULATION);
        init();
    }

    private void init() {
        this.controllers = new ArrayList<>();
        this.controlledProcesses = new ArrayList<>();
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

    public String getControlType() {
        return controlType;
    }

    public void setControlType(String controlType) {
        this.controlType = controlType;
    }
}
