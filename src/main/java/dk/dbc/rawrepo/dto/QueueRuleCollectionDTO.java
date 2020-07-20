/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class QueueRuleCollectionDTO {
    List<QueueRuleDTO> queueRules;

    public List<QueueRuleDTO> getQueueRules() {
        return queueRules;
    }

    public void setQueueRules(List<QueueRuleDTO> queueRules) {
        this.queueRules = queueRules;
    }
}
