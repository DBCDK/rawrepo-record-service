package dk.dbc.rawrepo;

import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;

import java.util.ArrayList;
import java.util.List;

public class RecordBeanUtils {

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
