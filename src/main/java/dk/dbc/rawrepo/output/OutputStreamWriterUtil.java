package dk.dbc.rawrepo.output;

import dk.dbc.rawrepo.exception.WebApplicationInvalidInputException;

import java.io.OutputStream;

public class OutputStreamWriterUtil {

    public static OutputStreamRecordWriter getWriter(String type, OutputStream stream, String encoding) throws WebApplicationInvalidInputException {
        if (type.equalsIgnoreCase("LINE")) {
            return new OutputStreamLineRecordWriter(stream, encoding);
        }

        // TODO: 10/03/2022 should the JSON format be made to produce the same output as MARC_JSON at some point?
        if (type.equalsIgnoreCase("JSON")) {
            return new OutputStreamJsonRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("MARC_JSON")) {
            return new OutputStreamMarcJsonRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("MARCXCHANGE")) {
            return new OutputStreamMarcXchangeRecordWriter(stream, encoding);
        }

        throw new WebApplicationInvalidInputException("Unknown OutputStreamRecordWriter type");
    }
}
