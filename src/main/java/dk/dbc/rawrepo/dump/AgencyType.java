/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.RecordBeanUtils;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;

public enum AgencyType {
    DBC, FBS, LOCAL;

    public static AgencyType getAgencyType(VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector, int agencyId) throws VipCoreException {
        if (RecordBeanUtils.DBC_AGENCIES.contains(agencyId)) {
            return AgencyType.DBC;
        }

        boolean useEnrichments = vipCoreLibraryRulesConnector.hasFeature(agencyId, VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS);

        // Yes, 191919 is a DBC agency, however when dumping the records they should be treated as a local record as
        // they are pure enrichment records and can't be merged unless the parent record is dumped
        if (!useEnrichments || RecordBeanUtils.DBC_ENRICHMENT_AGENCY == agencyId) {
            return AgencyType.LOCAL;
        } else {
            return AgencyType.FBS;
        }
    }
}

