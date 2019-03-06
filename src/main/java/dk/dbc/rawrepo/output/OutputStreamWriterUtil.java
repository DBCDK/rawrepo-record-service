/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.output;

import dk.dbc.rawrepo.exception.WebApplicationInvalidInputException;

import java.io.OutputStream;

public class OutputStreamWriterUtil {

    public static OutputStreamRecordWriter getWriter(String type, OutputStream stream, String encoding) throws WebApplicationInvalidInputException {
        if (type.equalsIgnoreCase("LINE")) {
            return new OutputStreamLineRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("JSON")) {
            return new OutputStreamJsonRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("MARCXCHANGE")) {
            return new OutputStreamMarcXchangeRecordWriter(stream, encoding);
        }

        throw new WebApplicationInvalidInputException("Unknown OutputStreamRecordWriter type");
    }
}
