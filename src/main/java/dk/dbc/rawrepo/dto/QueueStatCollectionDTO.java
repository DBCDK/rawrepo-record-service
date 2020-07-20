/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class QueueStatCollectionDTO {

    private List<QueueStatDTO> queueStats;

    public List<QueueStatDTO> getQueueStats() {
        return queueStats;
    }

    public void setQueueStats(List<QueueStatDTO> queueStats) {
        this.queueStats = queueStats;
    }
}
