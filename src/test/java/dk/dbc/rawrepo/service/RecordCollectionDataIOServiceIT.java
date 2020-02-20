package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.marc.binding.MarcRecord;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RecordCollectionDataIOServiceIT extends AbstractRecordServiceContainerTest {
    private static final String BASE_DIR = "sql/collection-dataio/";
    private static final String BIBLIOGRAPHIC_RECORD_ID_HEAD = "50129691";
    private static final String BIBLIOGRAPHIC_RECORD_ID_VOLUME = "05395720";
    private static final int COMMON_AGENCY = 870970;
    private static final int COMMON_ENRICHMENT = 191919;
    private static final int FBS_AGENCY = 770600;

    private static Response callRecordService() {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", FBS_AGENCY)
                .bind("bibliographicrecordid", BIBLIOGRAPHIC_RECORD_ID_VOLUME);
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        return httpClient.execute(httpGet);
    }

    private static void reset(Connection rawrepoConnection) throws Exception {
        resetRawrepoDb(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-common.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "head-common-enrichment.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_ENRICHMENT, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-common.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "volume-common-enrichment.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_ENRICHMENT, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);
    }

    @Test
    void deletedVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-deleted-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void activeVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void activeHeadActiveVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void deletedHeadActiveVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void activeHeadDeletedVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        //saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void deletedHeadDeletedVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-deleted-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void activeHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void deletedHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final String collection = response.readEntity(String.class);

        final HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-deleted-merged.xml")));
    }

}
