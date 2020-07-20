/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

public class QueueRuleDTO {
    String provider;
    String worker;
    char changed;
    char leaf;
    String description;

    public QueueRuleDTO() {
    }

    public QueueRuleDTO(String provider, String worker, char changed, char leaf, String description) {
        this.provider = provider;
        this.worker = worker;
        this.changed = changed;
        this.leaf = leaf;
        this.description = description;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getWorker() {
        return worker;
    }

    public void setWorker(String worker) {
        this.worker = worker;
    }

    public char getChanged() {
        return changed;
    }

    public void setChanged(char changed) {
        this.changed = changed;
    }

    public char getLeaf() {
        return leaf;
    }

    public void setLeaf(char leaf) {
        this.leaf = leaf;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
