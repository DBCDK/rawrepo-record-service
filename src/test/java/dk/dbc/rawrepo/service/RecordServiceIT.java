package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RecordServiceIT extends AbstractRecordServiceContainerTest {

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
    void getRecord_Ok() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordDTO recordDTO = response.readEntity(RecordDTO.class);
        assertThat("get agencyId", recordDTO.getRecordId().getAgencyId(), is(191919));
        assertThat("get bibliographicRecordid", recordDTO.getRecordId().getBibliographicRecordId(), is("50129691"));
    }

    @Test
    void getRecord_NotFound() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}")
                        .bind("bibliographicRecordId", "NOTFOUND")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(Response.Status.NO_CONTENT.getStatusCode()));

        String entity = response.readEntity(String.class);
        assertThat("content", entity, is(""));
    }

    @Test
    void getMarcRecord_Merged() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .bind("mode", "merged")
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final byte[] content = response.readEntity(byte[].class);
        assertThat("content", getMarcRecordFromString(content), is(getMarcRecordFromFile("sql/50129691-191919-merged.xml")));
    }

    @Test
    void getMarcRecord_Merged_ExcludeDBCFields() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withQueryParameter("exclude-dbc-fields", "true")
                .withQueryParameter("mode", "merged")
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final byte[] content = response.readEntity(byte[].class);
        assertThat("content", getMarcRecordFromString(content), is(getMarcRecordFromFile("sql/50129691-191919-merged-exclude-dbc-fields.xml")));
    }

    @Test
    void getMarcRecord_Merged_UseParentAgency() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withQueryParameter("use-parent-agency", "true")
                .withQueryParameter("mode", "merged")
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final byte[] content = response.readEntity(byte[].class);
        assertThat("content", getMarcRecordFromString(content), is(getMarcRecordFromFile("sql/50129691-191919-merged-parent-agency.xml")));
    }

    @Test
    void getMarcRecordContent_OutputFormatJSON() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withQueryParameter("use-parent-agency", "true")
                .withQueryParameter("mode", "merged")
                .withQueryParameter("output-format", "json")
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final String content = response.readEntity(String.class);
        // Because we have told IntelliJ to always add an empty line to files, the expected json file will have an empty line
        // which is near impossible to get rid of. The simplest solution replace "\n" with nothing.
        String expected = new String(getContentFromFile("sql/50129691-191919-merged-parent-agency.json"));
        expected = expected.replace("\n", "");

        assertThat("content", content, is(expected));
    }

    @Test
    void getMarcRecordContent_OutputFormatLINE() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withQueryParameter("use-parent-agency", "true")
                .withQueryParameter("mode", "merged")
                .withQueryParameter("output-format", "line")
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));
        //response.getHeaders();

        final String content = response.readEntity(String.class);
        assertThat("content", content, is(new String(getContentFromFile("sql/50129691-191919-merged-parent-agency.txt"))));
    }

    @Test
    void getMarcRecord_NotFound() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/content")
                        .bind("bibliographicRecordId", "NOTFOUND")
                        .bind("agencyId", 191919)
                        .bind("mode", "merged")
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(204));

        String entity = response.readEntity(String.class);
        assertThat("content", entity, is(""));
    }

    @Test
    void getRecordMetaData_Found() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/meta")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        // TODO Assert the record value.
        // The object is a marshalled Record class but Record class is abstract which means it can't be unmarshalled easily
        //Record record = response.readEntity(Record.class);
        //assertThat("content", record, is(nullValue()));
    }

    @Test
    void getRecordMetaData_NotFound() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/meta")
                        .bind("bibliographicRecordId", "NOTFOUND")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(204));

        String entity = response.readEntity(String.class);
        assertThat("content", entity, is(""));
    }

    @Test
    void exists_Ok() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/exists")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordExistsDTO recordExistsDTO = response.readEntity(RecordExistsDTO.class);
        assertThat("get response", recordExistsDTO.isValue(), is(true));
    }

    @Test
    void exists_NotFound() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/exists")
                        .bind("bibliographicRecordId", "NOTFOUND")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordExistsDTO recordExistsDTO = response.readEntity(RecordExistsDTO.class);
        assertThat("get response", recordExistsDTO.isValue(), is(false));
    }

    @Test
    void getRelationsParents_NoParents_MarcXChange() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/parents")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 870970)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordIdCollectionDTO recordIdCollectionDTO = response.readEntity(RecordIdCollectionDTO.class);
        assertThat("get response", recordIdCollectionDTO.getRecordIds().size(), is(0));
    }

    @Test
    void getRelationsParents_NoParents_Enrichment() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/parents")
                        .bind("bibliographicRecordId", "50129691")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        RecordIdCollectionDTO recordIdCollectionDTO = response.readEntity(RecordIdCollectionDTO.class);
        assertThat("get response", recordIdCollectionDTO.getRecordIds().size(), is(0));
    }

    // TODO When a parent child relation is added to this class at some point the getRelationsPatents should be updated

    @Test
    void getRelationsParents_NotFound() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/parents")
                        .bind("bibliographicRecordId", "NOTFOUND")
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(204));

        String entity = response.readEntity(String.class);
        assertThat("content", entity, is(""));
    }

}
