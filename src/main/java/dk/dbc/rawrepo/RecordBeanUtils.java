/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordBeanUtils {

    public static final int DBC_ENRICHMENT_AGENCY = 191919;
    public static final List<Integer> DBC_AGENCIES = Arrays.asList(870970, 870971, 870974, 870979, 190002, 190004);
    public static final List<Integer> DBC_AGENCIES_ALL = Arrays.asList(191919, 870970, 870971, 870974, 870979, 190002, 190004);

    public static MarcRecord removePrivateFields(MarcRecord marcRecord) {
        final List<Field> fields = new ArrayList<>();

        for (Field field : marcRecord.getFields()) {
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
