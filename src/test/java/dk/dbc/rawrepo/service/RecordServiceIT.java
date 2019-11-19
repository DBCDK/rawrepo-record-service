/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */


package dk.dbc.rawrepo.service;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

import static org.hamcrest.core.Is.is;

public class RecordServiceIT {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordServiceIT.class);

    private Connection connection;
    private PostgresITConnection postgres;
    private RecordService recordService;

    @Before
    public void setup() throws SQLException {
        postgres = new PostgresITConnection("rawrepo");
        connection = postgres.getConnection();
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        postgres.close();
    }

    private void resetDatabase() throws SQLException {
        System.out.println("resetDatabase");

        postgres.clearTables("relations", "records", "records_cache", "records_archive", "queue", "jobdiag");
    }

    private byte[] fileToContent(String filename) throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream(filename);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = bufferedInputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        return buffer.toByteArray();
    }

    /*
      bibliographicrecordid VARCHAR(64)              NOT NULL,
      agencyid              NUMERIC(6)               NOT NULL,
      deleted               BOOLEAN                  NOT NULL DEFAULT FALSE, -- V3
      mimetype              VARCHAR(128)             NOT NULL DEFAULT 'text/marcxchange', -- V3
      content               TEXT, -- base64 encoded
      created               TIMESTAMP WITH TIME ZONE NOT NULL,
      modified              TIMESTAMP WITH TIME ZONE NOT NULL,
      trackingId            VARCHAR(256)             NOT NULL DEFAULT '',
     */

    private void insertRecord(String bibliographicRecordId, int agencyId, String mimeType, byte[] content) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO records(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        int i = 1;
        stmt.setString(i++, bibliographicRecordId);
        stmt.setInt(i++, agencyId);
        stmt.setString(i++, "f");
        stmt.setString(i++, mimeType);
        stmt.setBytes(i++, content);
        stmt.setString(i++, Instant.now().toString());
        stmt.setString(i++, Instant.now().toString());
        stmt.setString(i, "");
        stmt.execute();
    }

    @Test
    public void recordTest() throws Exception {
        byte[] content = fileToContent("sql/50129691-191919.sql");

        insertRecord("50129691", 191919, "enrichment", content);

        recordService = new RecordService();

        Assert.assertThat(recordService.recordExists(191919, "50129691", false), is(false));
    }

}
