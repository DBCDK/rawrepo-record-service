/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class QueueProviderCollectionDTO {

    List<String> providers;

    public List<String> getProviders() {
        return providers;
    }

    public void setProviders(List<String> providers) {
        this.providers = providers;
    }
}
