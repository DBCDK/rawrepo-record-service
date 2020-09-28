/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.rawrepo.RecordDTOCollection;
import dk.dbc.rawrepo.dto.RecordDTO;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RecordCollectionServiceIT extends AbstractRecordServiceContainerTest {

    @BeforeAll
    static void initDB() {

        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoDb(rawrepoConnection);

            saveRecord(rawrepoConnection, "sql/collection/703213855-830380.xml", MIMETYPE_MARCXCHANGE);
            saveRecord(rawrepoConnection, "sql/collection/285927994-830380.xml", MIMETYPE_MARCXCHANGE);
            saveRelations(rawrepoConnection, "285927994", 830380, "703213855", 830380);

            saveRecord(rawrepoConnection, "sql/collection/50129691-870970.xml", MIMETYPE_MARCXCHANGE); // Head
            saveRecord(rawrepoConnection, "sql/collection/50129691-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/collection/50129691-770700.xml", MIMETYPE_ENRICHMENT);
            saveRelations(rawrepoConnection, "50129691", 191919, "50129691", 870970);
            // No relation from 50129691:770700 to 50129691:870970 as the 770700 record is deleted

            saveRecord(rawrepoConnection, "sql/collection/05395720-870970.xml", MIMETYPE_MARCXCHANGE); // Volume
            saveRecord(rawrepoConnection, "sql/collection/05395720-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/collection/05395720-770700.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/collection/05395720-770600.xml", MIMETYPE_ENRICHMENT);
            saveRelations(rawrepoConnection, "05395720", 191919, "05395720", 870970);
            saveRelations(rawrepoConnection, "05395720", 770700, "05395720", 870970);
            // No relation from 05395720:770600 to 50129691:870970 as the 770600 record is deleted
            saveRelations(rawrepoConnection, "05395720", 870970, "50129691", 870970); // volume -> head

            saveRecord(rawrepoConnection, "sql/collection/05395721-870970.xml", MIMETYPE_MARCXCHANGE); // Deleted
            saveRecord(rawrepoConnection, "sql/collection/05395721-191919.xml", MIMETYPE_ENRICHMENT); // Deleted
            saveRecord(rawrepoConnection, "sql/collection/05395721-770600.xml", MIMETYPE_ENRICHMENT); // Deleted
            // No relations as every 05395721 records are deleted

            saveRecord(rawrepoConnection, "sql/collection/37408115-870971.xml", MIMETYPE_ARTICLE); // Article, deleted
            saveRecord(rawrepoConnection, "sql/collection/37408115-191919.xml", MIMETYPE_ENRICHMENT); // Article, deleted
            saveRecord(rawrepoConnection, "sql/collection/52451302-870970.xml", MIMETYPE_MARCXCHANGE); // Volume, deleted
            saveRecord(rawrepoConnection, "sql/collection/52451302-191919.xml", MIMETYPE_ENRICHMENT); // Volume, deleted
            saveRecord(rawrepoConnection, "sql/collection/51080157-870970.xml", MIMETYPE_MARCXCHANGE); // Head
            saveRecord(rawrepoConnection, "sql/collection/51080157-191919.xml", MIMETYPE_ENRICHMENT); // Head
            saveRelations(rawrepoConnection, "51080157", 191919, "51080157", 870970);

            saveRecord(rawrepoConnection, "sql/collection/27218865-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/collection/27218865-870970.xml", MIMETYPE_MARCXCHANGE);
            saveRelations(rawrepoConnection, "27218865", 191919, "27218865", 870970);

            saveRecord(rawrepoConnection, "sql/collection/30707605-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/collection/30707605-870976.xml", MIMETYPE_MATVURD);
            saveRelations(rawrepoConnection, "30707605", 191919, "30707605", 870976);
            saveRelations(rawrepoConnection, "30707605", 870976, "27218865", 870970);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void getRecordCollection_DataIO_Ok() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "830380")
                .bind("bibliographicrecordid", "285927994");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 285927994", actual.containsKey("285927994"), is(true));
        assertThat("collection content 285927994", getMarcRecordFromString(actual.get("285927994").getContent()), is(getMarcRecordFromFile("sql/collection/285927994-830380.xml")));
        assertThat("collection contains 703213855", actual.containsKey("703213855"), is(true));
        assertThat("collection content 703213855", getMarcRecordFromString(actual.get("703213855").getContent()), is(getMarcRecordFromFile("sql/collection/703213855-830380.xml")));
    }

    @Test
    void getRecordCollection_DataIO_NotFound() {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "830380")
                .bind("bibliographicrecordid", "NOTFOUND");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(204));

        String entity = response.readEntity(String.class);
        assertThat("content", entity, CoreMatchers.is(""));
    }

    @Test
    void getRecordCollection_DataIO_LocalVolume() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "830380")
                .bind("bibliographicrecordid", "285927994");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 285927994", actual.containsKey("285927994"), is(true));
        assertThat("collection content 285927994", getMarcRecordFromString(actual.get("285927994").getContent()), is(getMarcRecordFromFile("sql/collection/285927994-830380.xml")));
        assertThat("collection contains 703213855", actual.containsKey("703213855"), is(true));
        assertThat("collection content 703213855", getMarcRecordFromString(actual.get("703213855").getContent()), is(getMarcRecordFromFile("sql/collection/703213855-830380.xml")));
    }

    @Test
    void getRecordCollection_DataIO_DeletedHead() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "770700")
                .bind("bibliographicrecordid", "05395720");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 05395720", actual.containsKey("05395720"), is(true));
        assertThat("collection content 05395720", getMarcRecordFromString(actual.get("05395720").getContent()), is(getMarcRecordFromFile("sql/collection/05395720-770700-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", getMarcRecordFromString(actual.get("50129691").getContent()), is(getMarcRecordFromFile("sql/collection/50129691-870970.xml")));
    }

    @Test
    void getRecordCollection_Common_DeletedHead() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);
        params.put("allow-deleted", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/content")
                .bind("agencyid", "770700")
                .bind("bibliographicrecordid", "05395720");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains 05395720", actual.containsKey("05395720"), is(true));
        assertThat("collection content 05395720", actual.get("05395720"), is(getMarcRecordFromFile("sql/collection/05395720-770700-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", actual.get("50129691"), is(getMarcRecordFromFile("sql/collection/50129691-770700-merged.xml")));
    }

    @Test
    void getRecordCollection_DataIO_DeletedVolume() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "770600")
                .bind("bibliographicrecordid", "05395720");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 05395720", actual.containsKey("05395720"), is(true));
        assertThat("collection content 05395720", getMarcRecordFromString(actual.get("05395720").getContent()), is(getMarcRecordFromFile("sql/collection/05395720-770600-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", getMarcRecordFromString(actual.get("50129691").getContent()), is(getMarcRecordFromFile("sql/collection/50129691-870970.xml")));
    }

    @Test
    void getRecordCollection_DataIO_DeletedVolume_ForCorepo() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "770600")
                .bind("bibliographicrecordid", "05395720");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 05395720", actual.containsKey("05395720"), is(true));
        assertThat("collection content 05395720", getMarcRecordFromString(actual.get("05395720").getContent()), is(getMarcRecordFromFile("sql/collection/05395720-770600-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", getMarcRecordFromString(actual.get("50129691").getContent()), is(getMarcRecordFromFile("sql/collection/50129691-870970.xml")));
    }

    @Test
    void getRecordCollection_DataIO_DeletedCommonVolume() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "770600")
                .bind("bibliographicrecordid", "05395721");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 05395720", actual.containsKey("05395721"), is(true));
        assertThat("collection content 05395720", getMarcRecordFromString(actual.get("05395721").getContent()), is(getMarcRecordFromFile("sql/collection/05395721-770600-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", getMarcRecordFromString(actual.get("50129691").getContent()), is(getMarcRecordFromFile("sql/collection/50129691-870970.xml")));
    }

    @Test
    void getRecordCollection_DataIO_DeletedCommonVolume_ForCorepo() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", "770600")
                .bind("bibliographicrecordid", "05395721");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains 05395720", actual.containsKey("05395721"), is(true));
        assertThat("collection content 05395720", getMarcRecordFromString(actual.get("05395721").getContent()), is(getMarcRecordFromFile("sql/collection/05395721-770600-merged.xml")));
        assertThat("collection contains 50129691", actual.containsKey("50129691"), is(true));
        assertThat("collection content 50129691", getMarcRecordFromString(actual.get("50129691").getContent()), is(getMarcRecordFromFile("sql/collection/50129691-870970.xml")));
    }

    @Test
    void getRecordCollection_RawrepoSolrIndexerBasis_Article() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("allow-deleted", true);
        params.put("use-parent-agency", true);
        params.put("expand", true);
        params.put("keep-aut-fields", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/content")
                .bind("agencyid", "191919")
                .bind("bibliographicrecordid", "37408115");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains 05395720", actual.containsKey("37408115"), is(true));
        assertThat("collection content 05395720", actual.get("37408115"), is(getMarcRecordFromFile("sql/collection/37408115-870971.xml")));
        assertThat("collection contains 50129691", actual.containsKey("52451302"), is(true));
        assertThat("collection content 50129691", actual.get("52451302"), is(getMarcRecordFromFile("sql/collection/52451302-870970.xml")));
        assertThat("collection contains 50129691", actual.containsKey("51080157"), is(true));
        assertThat("collection content 50129691", actual.get("51080157"), is(getMarcRecordFromFile("sql/collection/51080157-870970.xml")));
    }

    @Test
    void getRecordCollection_RawrepoSolrIndexerBasis_MatVurd() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("allow-deleted", true);
        params.put("use-parent-agency", true);
        params.put("expand", true);
        params.put("keep-aut-fields", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/content")
                .bind("agencyid", "191919")
                .bind("bibliographicrecordid", "30707605");
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains 30707605", actual.containsKey("30707605"), is(true));
        assertThat("collection content 30707605", actual.get("30707605"), is(getMarcRecordFromFile("sql/collection/30707605-870976-merged.xml")));
        assertThat("collection contains 27218865", actual.containsKey("27218865"), is(true));
        assertThat("collection content 27218865", actual.get("27218865"), is(getMarcRecordFromFile("sql/collection/27218865-870970.xml")));
    }

}
