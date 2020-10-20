/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.MarcRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecordBeanUtils {

    public static final int DBC_ENRICHMENT_AGENCY = 191919;
    public static final List<Integer> DBC_AGENCIES = Collections.unmodifiableList(Arrays.asList(870970, 870971, 870974, 870979, 190002, 190004));

    public static MarcRecord removePrivateFields(MarcRecord marcRecord) {
        final List<DataField> fields = new ArrayList<>();

        for (DataField field : marcRecord.getFields(DataField.class)) {
            if (field.getTag().matches("^[0-9].*")) {
                fields.add(field);
            }
        }

        final MarcRecord newMarcRecord = new MarcRecord();
        newMarcRecord.setLeader(marcRecord.getLeader());
        newMarcRecord.addAllFields(fields);

        return newMarcRecord;
    }
}
