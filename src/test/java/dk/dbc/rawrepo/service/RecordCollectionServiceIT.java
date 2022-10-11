package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.rawrepo.RecordDTOCollection;
import dk.dbc.rawrepo.dto.RecordCollectionDTOv2;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

            saveRecord(rawrepoConnection, "sql/collection/30707605-191919.xml", MIMETYPE_ENRICHMENT, "2020-10-19T09:42:42.000Z", "2020-10-19T09:43:42.000Z");
            saveRecord(rawrepoConnection, "sql/collection/30707605-870976.xml", MIMETYPE_MATVURD, "2020-10-19T09:44:42.000Z", "2020-10-19T09:45:42.000Z");
            saveRelations(rawrepoConnection, "30707605", 191919, "30707605", 870976);
            saveRelations(rawrepoConnection, "30707605", 870976, "27218865", 870970);

            saveRecord(rawrepoConnection, "sql/collection/68733324-870979.xml", MIMETYPE_AUTHORITY);
            saveRecord(rawrepoConnection, "sql/collection/68733324-191919.xml", MIMETYPE_ENRICHMENT);
            saveRelations(rawrepoConnection, "68733324", 191919, "68733324", 870979);
            saveRecord(rawrepoConnection, "sql/collection/04628543-870970.xml", MIMETYPE_MARCXCHANGE);
            saveRecord(rawrepoConnection, "sql/collection/04628543-191919.xml", MIMETYPE_ENRICHMENT);
            saveRelations(rawrepoConnection, "04628543", 191919, "04628543", 870970);
            saveRelations(rawrepoConnection, "04628543", 870970, "68733324", 870979);
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

    @Test
    void fetchRecordCollection_Raw() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("exclude-attribute", "contentJSON");

        final RecordIdCollectionDTO recordIdCollectionDTO = new RecordIdCollectionDTO();
        List<RecordIdDTO> recordIdDTOList = new ArrayList<>();
        recordIdDTOList.add(new RecordIdDTO("30707605", 870976));
        recordIdDTOList.add(new RecordIdDTO("27218865", 870970));
        recordIdDTOList.add(new RecordIdDTO("not found", 123456));
        recordIdCollectionDTO.setRecordIds(recordIdDTOList);

        final PathBuilder path = new PathBuilder("/api/v1/records/fetch");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(recordIdCollectionDTO, MediaType.APPLICATION_JSON);
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }
        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final RecordCollectionDTOv2 actual = response.readEntity(RecordCollectionDTOv2.class);

        RecordDTO recordDTO = actual.getFound().get(0);
        assertThat("collection contains 30707605", recordDTO.getRecordId(), is(new RecordIdDTO("30707605", 870976)));
        assertThat("collection mimetype 30707605", recordDTO.getMimetype(), is("text/matvurd+marcxchange"));
        assertThat("collection created 30707605", recordDTO.getCreated(), is("2020-10-19T09:44:42Z"));
        assertThat("collection modified 30707605", recordDTO.getModified(), is("2020-10-19T09:45:42Z"));
        assertThat("collection enrichment trail 30707605", recordDTO.getEnrichmentTrail(), is("870976"));
        assertThat("collection content 30707605", byteArrayToRecord(recordDTO.getContent()), is(getMarcRecordFromFile("sql/collection/30707605-870976.xml")));


        recordDTO = actual.getFound().get(1);
        assertThat("collection contains 27218865", recordDTO.getRecordId(), is(new RecordIdDTO("27218865", 870970)));
        assertThat("collection mimetype 27218865", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection enrichment trail 27218865", recordDTO.getEnrichmentTrail(), is("870970"));
        assertThat("collection content 27218865", byteArrayToRecord(recordDTO.getContent()), is(getMarcRecordFromFile("sql/collection/27218865-870970.xml")));

        assertThat("collection is missing record", actual.getMissing().size(), is(1));
        assertThat("collection is missing record", actual.getMissing().get(0), is(new RecordIdDTO("not found", 123456)));
    }

    @Test
    void fetchRecordCollection_Merged() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("mode", "merged");
        params.put("use-parent-agency", "true");
        params.put("exclude-attribute", "contentJSON");

        final RecordIdCollectionDTO recordIdCollectionDTO = new RecordIdCollectionDTO();
        List<RecordIdDTO> recordIdDTOList = new ArrayList<>();
        recordIdDTOList.add(new RecordIdDTO("30707605", 191919));
        recordIdDTOList.add(new RecordIdDTO("27218865", 191919));
        recordIdDTOList.add(new RecordIdDTO("not found", 123456));
        recordIdCollectionDTO.setRecordIds(recordIdDTOList);

        final PathBuilder path = new PathBuilder("/api/v1/records/fetch");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(recordIdCollectionDTO, MediaType.APPLICATION_JSON);
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }
        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final RecordCollectionDTOv2 actual = response.readEntity(RecordCollectionDTOv2.class);

        RecordDTO recordDTO = actual.getFound().get(0);
        assertThat("collection contains 30707605", recordDTO.getRecordId(), is(new RecordIdDTO("30707605", 191919)));
        assertThat("collection mimetype 30707605", recordDTO.getMimetype(), is("text/matvurd+marcxchange"));
        assertThat("collection created 30707605", recordDTO.getCreated(), is("2020-10-19T09:44:42Z"));
        assertThat("collection modified 30707605", recordDTO.getModified(), is("2020-10-19T09:45:42Z"));
        assertThat("collection enrichment trail 30707605", recordDTO.getEnrichmentTrail(), is("870976,191919"));
        assertThat("collection content 30707605", byteArrayToRecord(recordDTO.getContent()), is(getMarcRecordFromFile("sql/collection/30707605-870976-merged.xml")));

        recordDTO = actual.getFound().get(1);
        assertThat("collection contains 27218865", recordDTO.getRecordId(), is(new RecordIdDTO("27218865", 191919)));
        assertThat("collection mimetype 27218865", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection enrichment trail 27218865", recordDTO.getEnrichmentTrail(), is("870970,191919"));
        assertThat("collection content 27218865", byteArrayToRecord(recordDTO.getContent()), is(getMarcRecordFromFile("sql/collection/27218865-870970.xml")));

        assertThat("collection is missing record", actual.getMissing().size(), is(1));
        assertThat("collection is missing record", actual.getMissing().get(0), is(new RecordIdDTO("not found", 123456)));
    }

    @Test
    void getRecordsBulkv2_Merged() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("use-parent-agency", "true");

        final RecordIdCollectionDTO recordIdCollectionDTO = new RecordIdCollectionDTO();
        final List<RecordIdDTO> recordIdDTOList = new ArrayList<>();
        recordIdDTOList.add(new RecordIdDTO("04628543", 191919));
        recordIdCollectionDTO.setRecordIds(recordIdDTOList);

        final PathBuilder path = new PathBuilder("/api/v2/records/bulk");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(recordIdCollectionDTO, MediaType.APPLICATION_JSON);
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }
        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        String expected = "001 00 *a04628543*b870970*c20200121012638*d19860731*fa\n" +
                "004 00 *rn*ab\n" +
                "008 00 *tm*uf*a1975*leng*v1\n" +
                "245 00 *G1-1*gVolumen 1*aTriodium athoum, pars principalis*cCodex Monasterii Va\n" +
                "    topedii 1488 phototypice depictus*eedendum curaverunt: Enrica Follieri et O\n" +
                "    liver Strunk\n" +
                "700 00 *5870979*668733324\n" +
                "996 00 *aDBC\n" +
                "$";

        try (InputStream inputStream = response.readEntity(InputStream.class)) {
            String result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));

            assertThat(result, CoreMatchers.is(expected));
        }
    }

    @Test
    void getRecordsBulkv2_Expanded() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("use-parent-agency", "true");
        params.put("expand", "true");

        final RecordIdCollectionDTO recordIdCollectionDTO = new RecordIdCollectionDTO();
        final List<RecordIdDTO> recordIdDTOList = new ArrayList<>();
        recordIdDTOList.add(new RecordIdDTO("04628543", 191919));
        recordIdCollectionDTO.setRecordIds(recordIdDTOList);

        final PathBuilder path = new PathBuilder("/api/v2/records/bulk");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(recordIdCollectionDTO, MediaType.APPLICATION_JSON);
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }
        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        String expected = "001 00 *a04628543*b870970*c20200121012638*d19860731*fa\n" +
                "004 00 *rn*ab\n" +
                "008 00 *tm*uf*a1975*leng*v1\n" +
                "245 00 *G1-1*gVolumen 1*aTriodium athoum, pars principalis*cCodex Monasterii Va\n" +
                "    topedii 1488 phototypice depictus*eedendum curaverunt: Enrica Follieri et O\n" +
                "    liver Strunk\n" +
                "700 00 *aMurakami*hHaruki\n" +
                "996 00 *aDBC\n" +
                "$";

        try (InputStream inputStream = response.readEntity(InputStream.class)) {
            String result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));

            assertThat(result, CoreMatchers.is(expected));
        }
    }

    private MarcRecord byteArrayToRecord(byte[] content) throws MarcReaderException {
        final InputStream inputStream = new ByteArrayInputStream(content);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        return reader.read();
    }

}
