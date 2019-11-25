/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.sql.Connection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RecordServiceIT extends AbstractRecordServiceContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRecordServiceContainerTest.class);

    @BeforeAll
    static void initDB() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoDb(rawrepoConnection);

            saveRecord(rawrepoConnection, "sql/50129691-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/50129691-870970.xml", MIMETYPE_MARCXCHANGE);
            saveRelations(rawrepoConnection, "50129691", 191919, "50129691", 870970);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void getRecord() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}")
                        .bind("bibliographicRecordId", 50129691)
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordDTO recordDTO = response.readEntity(RecordDTO.class);
        assertThat("get agencyId", recordDTO.getRecordId().getAgencyId(), is(191919));
        assertThat("get bibliographicRecordid", recordDTO.getRecordId().getBibliographicRecordId(), is("50129691"));
    }

    @Test
    void exists() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/exists")
                        .bind("bibliographicRecordId", 50129691)
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordExistsDTO recordExistsDTO = response.readEntity(RecordExistsDTO.class);
        assertThat("get response", recordExistsDTO.isValue(), is(true));
    }
}
