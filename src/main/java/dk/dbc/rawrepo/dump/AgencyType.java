/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RecordBeanUtils;

public enum AgencyType {
    DBC, FBS, LOCAL;

    public static AgencyType getAgencyType(OpenAgencyServiceFromURL openAgencyServiceFromURL, int agencyId) throws OpenAgencyException {
        if (RecordBeanUtils.DBC_AGENCIES.contains(agencyId)) {
            return AgencyType.DBC;
        }

        boolean useEnrichments = openAgencyServiceFromURL.libraryRules().isAllowed(agencyId, LibraryRuleHandler.Rule.USE_ENRICHMENTS);

        // Yes, 191919 is a DBC agency, however when dumping the records they should be treated as a local record as
        // they are pure enrichment records and can't be merged unless the parent record is dumped
        if (!useEnrichments || RecordBeanUtils.DBC_ENRICHMENT_AGENCY == agencyId) {
            return AgencyType.LOCAL;
        } else {
            return AgencyType.FBS;
        }
    }
}

