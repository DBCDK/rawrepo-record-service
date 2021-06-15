/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class RecordObjectMapper {

    private RecordObjectMapper() {

    }

    private static final MarcXchangeV1Writer marcRecordWriter = new MarcXchangeV1Writer();
    private static final Charset charset = StandardCharsets.UTF_8;

    public static byte[] marcToContent(MarcRecord marcRecord) {
        return marcRecordWriter.write(marcRecord, charset);
    }

    public static byte[] marcRecordCollectionToContent(Collection<MarcRecord> marcRecords) {
        return marcRecordWriter.writeCollection(marcRecords, charset);
    }

    public static MarcRecord contentToMarcRecord(byte[] content) throws MarcReaderException {
        final InputStream inputStream = new ByteArrayInputStream(content);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, charset);

        return reader.read();
    }

}
