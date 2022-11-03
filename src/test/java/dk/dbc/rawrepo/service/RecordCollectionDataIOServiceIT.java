package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.RecordDTOCollection;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.dto.RecordDTO;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RecordCollectionDataIOServiceIT extends AbstractRecordServiceContainerTest {
    private static final String BASE_DIR = "sql/collection-dataio/";
    private static final String BIBLIOGRAPHIC_RECORD_ID_HEAD = "50129691";
    private static final String BIBLIOGRAPHIC_RECORD_ID_VOLUME = "05395720";
    private static final String BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME = "113782544";
    private static final String BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD = "113778148";
    private static final int COMMON_AGENCY = 870970;
    private static final int AUTHORITY_AGENCY = 870979;
    private static final int COMMON_ENRICHMENT = 191919;
    private static final int FBS_AGENCY = 770600;
    private static final int LOCAL_AGENCY = 746100;

    private static Response callRecordService() {
        return callRecordService(BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY);
    }

    private static Response callRecordService(String bibliographicRecordId, int agencyId) {
        return callRecordService(bibliographicRecordId, agencyId, false, false);
    }

    private static Response callRecordService(String bibliographicRecordId, int agencyId, boolean handleControlRecords, boolean useParentAgency) {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);
        params.put("handle-control-records", handleControlRecords);
        params.put("use-parent-agency", useParentAgency);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", agencyId)
                .bind("bibliographicrecordid", bibliographicRecordId);
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

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-deleted-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void activeVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
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

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void deletedHeadActiveVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        saveRecord(rawrepoConnection, BASE_DIR + "volume-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_VOLUME, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        final Map<String, RecordDTO> actual;
        try (Response response = callRecordService()) {

            assertThat("Response code", response.getStatus(), is(200));

            actual = response.readEntity(RecordDTOCollection.class).toMap();
        }
        System.out.println(actual.toString());
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-active-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
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

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void activeCommonHeadDeletedCommonVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        resetRawrepoDb(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "volume-common-deleted.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "volume-common-enrichment-deleted.xml", MIMETYPE_ENRICHMENT);
        //saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        saveRecord(rawrepoConnection, BASE_DIR + "head-common.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "head-common-enrichment.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_ENRICHMENT, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        final Response response = callRecordService(BIBLIOGRAPHIC_RECORD_ID_VOLUME, COMMON_AGENCY);

        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-common-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
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

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-fbs-deleted-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-merged.xml")));
    }

    @Test
    void deletedCommonHeadDeletedCommonVolume() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        // We don't want the active common records to only delete the rows, don't initialize
        resetRawrepoDb(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-common-deleted.xml", MIMETYPE_MARCXCHANGE);
        // No relations as the record is deleted

        saveRecord(rawrepoConnection, BASE_DIR + "volume-common-deleted.xml", MIMETYPE_MARCXCHANGE);
        // No relations as the record is deleted

        // callRecordService requests the fbs agency so since we want the 870970
        final HashMap<String, Object> params = new HashMap<>();
        params.put("expand", true);

        final PathBuilder path = new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/dataio")
                .bind("agencyid", 870970)
                .bind("bibliographicrecordid", BIBLIOGRAPHIC_RECORD_ID_VOLUME);
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build());
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpGet.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpGet);

        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-common-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-common-deleted.xml")));
    }

    @Test
    void activeHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-active.xml", MIMETYPE_ENRICHMENT);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_HEAD, FBS_AGENCY, BIBLIOGRAPHIC_RECORD_ID_HEAD, COMMON_AGENCY);

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-active-merged.xml")));
    }

    @Test
    void deletedHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);

        saveRecord(rawrepoConnection, BASE_DIR + "head-fbs-deleted.xml", MIMETYPE_ENRICHMENT);
        // No relations as the record is deleted

        final Response response = callRecordService();

        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "volume-common-merged.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "head-fbs-deleted-merged.xml")));
    }

    @Test
    void deletedLocalVolumeDeletedLocalHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "local-volume-deleted.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "local-head-deleted.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME, LOCAL_AGENCY);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-volume-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-head-deleted.xml")));
    }

    @Test
    void deletedLocalVolumeActiveLocalHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "local-volume-deleted.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "local-head-active.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME, LOCAL_AGENCY);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-volume-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-head-active.xml")));
    }

    @Test
    void activeLocalVolumeActiveLocalHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "local-volume-active.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "local-head-active.xml", MIMETYPE_MARCXCHANGE);
        saveRelations(rawrepoConnection, BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME, LOCAL_AGENCY, BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD, LOCAL_AGENCY);

        final Response response = callRecordService(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME, LOCAL_AGENCY);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection contains volume", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_VOLUME).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-volume-active.xml")));
        assertThat("collection contains head", actual.containsKey(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(BIBLIOGRAPHIC_RECORD_ID_LOCAL_HEAD).getContent()), is(getMarcRecordFromFile(BASE_DIR + "local-head-active.xml")));
    }

    // There is no testcase testing combination of active local volume and deleted local head as that scenario is illegal

    @Test
    void activeFFUVolumeHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String bibliographicRecordIdVolume = "25196287";
        final String bibliographicRecordIdHead = "24050866";
        final int agencyId = 830600;

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-volume-active.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-head-active.xml", MIMETYPE_MARCXCHANGE);
        saveRelations(rawrepoConnection, bibliographicRecordIdVolume, agencyId, bibliographicRecordIdHead, agencyId);

        final Response response = callRecordService(bibliographicRecordIdVolume, agencyId);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection size", actual.size(), is(2));
        assertThat("collection contains volume", actual.containsKey(bibliographicRecordIdVolume), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(bibliographicRecordIdVolume).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-volume-active.xml")));
        assertThat("collection contains head", actual.containsKey(bibliographicRecordIdHead), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(bibliographicRecordIdHead).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-head-active.xml")));
    }

    @Test
    void deletedFFUVolumeActiveHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String bibliographicRecordIdVolume = "25196287";
        final String bibliographicRecordIdHead = "24050866";
        final int agencyId = 830600;

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-volume-deleted.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-head-active.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(bibliographicRecordIdVolume, agencyId);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection size", actual.size(), is(2));
        assertThat("collection contains volume", actual.containsKey(bibliographicRecordIdVolume), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(bibliographicRecordIdVolume).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-volume-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(bibliographicRecordIdHead), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(bibliographicRecordIdHead).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-head-active.xml")));
    }

    @Test
    void deletedFFUVolumeDeletedHead() throws Exception {
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String bibliographicRecordIdVolume = "25196287";
        final String bibliographicRecordIdHead = "24050866";
        final int agencyId = 830600;


        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-volume-deleted.xml", MIMETYPE_MARCXCHANGE);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-head-deleted.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(bibliographicRecordIdVolume, agencyId);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection size", actual.size(), is(2));
        assertThat("collection contains volume", actual.containsKey(bibliographicRecordIdVolume), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(bibliographicRecordIdVolume).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-volume-deleted.xml")));
        assertThat("collection contains head", actual.containsKey(bibliographicRecordIdHead), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(bibliographicRecordIdHead).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-head-deleted.xml")));
    }

    @Test
    void activeFFURecordWithAuthority() throws Exception {
        // It is allowed for FFU records to have subfields *5 and *6 pointing at authority records, but the relations are not created.
        // When the record is active the relations are used to find parents, which there are none of
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String bibliographicRecordId = "39387956";
        final int agencyId = 850020;

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-single-active.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(bibliographicRecordId, agencyId);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection size", actual.size(), is(1));
        assertThat("collection contains record", actual.containsKey(bibliographicRecordId), is(true));
        assertThat("collection content record", getMarcRecordFromString(actual.get(bibliographicRecordId).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-single-active.xml")));
    }

    @Test
    void deletedFFURecordWithAuthority() throws Exception {
        // It is allowed for FFU records to have subfields *5 and *6 pointing at authority records, but the relations are not created.
        // When the record is deleted the record content is read to find parent relations, which means we have to skip fields
        // with authority links otherwise bad things will happen as the record <authority bibliographicrecordid>:<ffu-agencyid>
        // doesn't exist
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String bibliographicRecordId = "39387956";
        final int agencyId = 850020;

        reset(rawrepoConnection);
        saveRecord(rawrepoConnection, BASE_DIR + "ffu-single-deleted.xml", MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(bibliographicRecordId, agencyId);

        assertThat("Response code", response.getStatus(), is(200));
        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("collection size", actual.size(), is(1));
        assertThat("collection contains record", actual.containsKey(bibliographicRecordId), is(true));
        assertThat("collection content record", getMarcRecordFromString(actual.get(bibliographicRecordId).getContent()), is(getMarcRecordFromFile(BASE_DIR + "ffu-single-deleted.xml")));
    }

    private void saveAuthority(Connection rawrepoConnection, String path, String id) throws Exception {
        saveRecord(rawrepoConnection, String.format("%s/%s-%s.xml", path, id, COMMON_ENRICHMENT), MIMETYPE_ENRICHMENT);
        saveRecord(rawrepoConnection, String.format("%s/%s-%s.xml", path, id, AUTHORITY_AGENCY), MIMETYPE_AUTHORITY);
        saveRelations(rawrepoConnection, id, COMMON_ENRICHMENT, id, AUTHORITY_AGENCY);
    }

    private void saveMarcXChange(Connection rawrepoConnection, String path, String id) throws Exception {
        saveRecord(rawrepoConnection, String.format("%s/%s-%s.xml", path, id, COMMON_ENRICHMENT), MIMETYPE_ENRICHMENT);
        saveRecord(rawrepoConnection, String.format("%s/%s-%s.xml", path, id, COMMON_AGENCY), MIMETYPE_MARCXCHANGE);
        saveRelations(rawrepoConnection, id, COMMON_ENRICHMENT, id, COMMON_AGENCY);
    }

    @Test
    void handleControlRecords_Scenario_1() throws Exception {
        /*
            Head + volume records where the volume points to a control volume record. Verify that the control hierarchy
            is returned.
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-1";
        final String authority1 = "68612888";
        final String authority2 = "68823498";
        final String head = "46185358";
        final String volume = "54627858";
        final String controlHead = "53514316";
        final String controlVolume = "54243677";

        resetRawrepoDb(rawrepoConnection);
        saveAuthority(rawrepoConnection, path, authority1);
        saveAuthority(rawrepoConnection, path, authority2);

        // Head
        saveMarcXChange(rawrepoConnection, path, head);
        saveRelations(rawrepoConnection, head, COMMON_AGENCY, Arrays.asList(
                new RecordId(authority1, AUTHORITY_AGENCY),
                new RecordId(authority2, AUTHORITY_AGENCY)));

        // Volume
        saveMarcXChange(rawrepoConnection, path, volume);
        saveRelations(rawrepoConnection, volume, COMMON_AGENCY, head, COMMON_AGENCY);

        // Control - head
        saveMarcXChange(rawrepoConnection, path, controlHead);
        saveRelations(rawrepoConnection, controlHead, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        // Control - volume
        saveMarcXChange(rawrepoConnection, path, controlVolume);
        saveRelations(rawrepoConnection, controlVolume, COMMON_AGENCY, controlHead, COMMON_AGENCY);

        final Response response = callRecordService(volume, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(4));

        assertThat("collection contains head", actual.containsKey(head), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(head).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, head))));

        assertThat("collection contains volume", actual.containsKey(volume), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(volume).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, volume))));

        assertThat("collection contains control head", actual.containsKey(controlHead), is(true));
        assertThat("collection content control head", getMarcRecordFromString(actual.get(controlHead).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, controlHead))));

        assertThat("collection contains control volume", actual.containsKey(controlVolume), is(true));
        assertThat("collection content control volume", getMarcRecordFromString(actual.get(controlVolume).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, controlVolume))));
    }

    @Test
    void handleControlRecords_Scenario_2() throws Exception {
        /*
            Simple test with single record and single control record
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-2";
        final String authority = "68594693";
        final String single = "38519387";
        final String control = "28947216";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, authority);

        // Single
        saveMarcXChange(rawrepoConnection, path, single);
        saveRelations(rawrepoConnection, single, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        // Control
        saveMarcXChange(rawrepoConnection, path, control);
        saveRelations(rawrepoConnection, control, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        final Response response = callRecordService(single, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(2));

        assertThat("collection contains single", actual.containsKey(single), is(true));
        assertThat("collection content single", getMarcRecordFromString(actual.get(single).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, single))));

        assertThat("collection contains control", actual.containsKey(control), is(true));
        assertThat("collection content control", getMarcRecordFromString(actual.get(control).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, control))));
    }

    @Test
    void handleControlRecords_Scenario_2_useParentAgency() throws Exception {
        /*
            Simple test with single record and single control record
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-2";
        final String authority = "68594693";
        final String single = "38519387";
        final String control = "28947216";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, authority);

        // Single
        saveMarcXChange(rawrepoConnection, path, single);
        saveRelations(rawrepoConnection, single, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        // Control
        saveMarcXChange(rawrepoConnection, path, control);
        saveRelations(rawrepoConnection, control, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        final Response response = callRecordService(single, COMMON_ENRICHMENT, true, true);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(2));

        assertThat("collection contains single", actual.containsKey(single), is(true));
        assertThat("collection content single", getMarcRecordFromString(actual.get(single).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-870970-expanded.xml", path, single))));

        assertThat("collection contains control", actual.containsKey(control), is(true));
        assertThat("collection content control", getMarcRecordFromString(actual.get(control).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-870970-expanded.xml", path, control))));
    }

    @Test
    void handleControlRecords_Scenario_2_deletedControlRecord() throws Exception {
        /*
            Simple test with single record and single control record where the control record is marked as deleted.
            Verify that the single record is returned only and no exception is thrown
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-2";
        final String authority = "68594693";
        final String single = "38519387";
        final String control = "28947216";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, authority);

        // Single
        saveMarcXChange(rawrepoConnection, path, single);
        saveRelations(rawrepoConnection, single, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        // Control
        saveRecord(rawrepoConnection, String.format("%s/%s-%s-deleted.xml", path, control, COMMON_ENRICHMENT), MIMETYPE_ENRICHMENT);
        saveRecord(rawrepoConnection, String.format("%s/%s-%s-deleted.xml", path, control, COMMON_AGENCY), MIMETYPE_MARCXCHANGE);

        final Response response = callRecordService(single, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(1));

        assertThat("collection contains single", actual.containsKey(single), is(true));
        assertThat("collection content single", getMarcRecordFromString(actual.get(single).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, single))));
    }

    @Test
    void handleControlRecords_Scenario_2_missingControlRecord() throws Exception {
        /*
            Simple test with single record and single control record where the control record is missing. Verify that
            the single record is returned only and no exception is thrown
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-2";
        final String authority = "68594693";
        final String single = "38519387";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, authority);

        // Single
        saveMarcXChange(rawrepoConnection, path, single);
        saveRelations(rawrepoConnection, single, COMMON_AGENCY, authority, AUTHORITY_AGENCY);

        final Response response = callRecordService(single, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(1));

        assertThat("collection contains single", actual.containsKey(single), is(true));
        assertThat("collection content single", getMarcRecordFromString(actual.get(single).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, single))));
    }

    @Test
    void handleControlRecords_Scenario_3() throws Exception {
        /*
            This tests a scenario where multiple records points to each other in 520 *n. Every record should be in the
            returned collection but only once.
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-3";
        final String theCurseOfTheBlackPearl = "38779877";
        final String deadMansChest = "38814168";
        final String atWorldsEnd = "38814192";
        final String onStrangerTides = "38814206";
        final String salazarsRevenge = "38814222";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, "69670601");
        saveAuthority(rawrepoConnection, path, "69679064");
        saveAuthority(rawrepoConnection, path, "69621317");
        saveAuthority(rawrepoConnection, path, "68886910");
        saveAuthority(rawrepoConnection, path, "68254329");
        saveAuthority(rawrepoConnection, path, "68280788");
        saveAuthority(rawrepoConnection, path, "69412610");
        saveAuthority(rawrepoConnection, path, "69598323");
        saveAuthority(rawrepoConnection, path, "69653197");
        saveAuthority(rawrepoConnection, path, "69736807");
        saveAuthority(rawrepoConnection, path, "69755631");

        final List<RecordId> authorityList = Arrays.asList(
                new RecordId("69670601", AUTHORITY_AGENCY),
                new RecordId("69679064", AUTHORITY_AGENCY),
                new RecordId("69621317", AUTHORITY_AGENCY),
                new RecordId("68886910", AUTHORITY_AGENCY),
                new RecordId("68254329", AUTHORITY_AGENCY),
                new RecordId("68280788", AUTHORITY_AGENCY),
                new RecordId("69412610", AUTHORITY_AGENCY),
                new RecordId("69598323", AUTHORITY_AGENCY),
                new RecordId("69653197", AUTHORITY_AGENCY),
                new RecordId("69736807", AUTHORITY_AGENCY),
                new RecordId("69755631", AUTHORITY_AGENCY));

        saveMarcXChange(rawrepoConnection, path, theCurseOfTheBlackPearl);
        saveMarcXChange(rawrepoConnection, path, deadMansChest);
        saveMarcXChange(rawrepoConnection, path, atWorldsEnd);
        saveMarcXChange(rawrepoConnection, path, onStrangerTides);
        saveMarcXChange(rawrepoConnection, path, salazarsRevenge);

        saveRelations(rawrepoConnection, theCurseOfTheBlackPearl, COMMON_AGENCY, authorityList);
        saveRelations(rawrepoConnection, deadMansChest, COMMON_AGENCY, authorityList);
        saveRelations(rawrepoConnection, atWorldsEnd, COMMON_AGENCY, authorityList);
        saveRelations(rawrepoConnection, onStrangerTides, COMMON_AGENCY, authorityList);
        saveRelations(rawrepoConnection, salazarsRevenge, COMMON_AGENCY, authorityList);

        final Response response = callRecordService(onStrangerTides, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(5));

        assertThat("collection contains theCurseOfTheBlackPearl", actual.containsKey(theCurseOfTheBlackPearl), is(true));
        assertThat("collection content theCurseOfTheBlackPearl", getMarcRecordFromString(actual.get(theCurseOfTheBlackPearl).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, theCurseOfTheBlackPearl))));
        assertThat("collection contains deadMansChest", actual.containsKey(deadMansChest), is(true));
        assertThat("collection content deadMansChest", getMarcRecordFromString(actual.get(deadMansChest).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, deadMansChest))));
        assertThat("collection contains atWorldsEnd", actual.containsKey(atWorldsEnd), is(true));
        assertThat("collection content atWorldsEnd", getMarcRecordFromString(actual.get(atWorldsEnd).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, atWorldsEnd))));
        assertThat("collection contains onStrangerTides", actual.containsKey(onStrangerTides), is(true));
        assertThat("collection content onStrangerTides", getMarcRecordFromString(actual.get(onStrangerTides).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, onStrangerTides))));
        assertThat("collection contains salazarsRevenge", actual.containsKey(salazarsRevenge), is(true));
        assertThat("collection content salazarsRevenge", getMarcRecordFromString(actual.get(salazarsRevenge).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, salazarsRevenge))));

        // For fun lets make the same call again but with handle-520n = false
        final Response responseWithout520n = callRecordService(onStrangerTides, COMMON_ENRICHMENT, false, false);
        assertThat("Response code Without520n", responseWithout520n.getStatus(), is(200));

        final Map<String, RecordDTO> actualWithout520n = responseWithout520n.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size Without520n", actualWithout520n.size(), is(1));
        assertThat("collection contains onStrangerTides Without520n", actual.containsKey(onStrangerTides), is(true));
        assertThat("collection content onStrangerTides Without520n", getMarcRecordFromString(actual.get(onStrangerTides).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, onStrangerTides))));
    }

    @Test
    void handleControlRecords_Scenario_4() throws Exception {
        /*
        Simple head + volume record where the volume record has multiple 520 *n references
         */
        final Connection rawrepoConnection = connectToRawrepoDb();
        final String path = "sql/collection-dataio-520n-4";
        final String authority1 = "19364364";
        final String authority2 = "19374610";
        final String head = "48678955";
        final String volume = "48473989";
        final String control1 = "54068239";
        final String control2 = "54135289";
        final String control3 = "54203063";
        final String control4 = "54329938";

        resetRawrepoDb(rawrepoConnection);

        // Authority
        saveAuthority(rawrepoConnection, path, authority1);
        saveAuthority(rawrepoConnection, path, authority2);

        // Head
        saveMarcXChange(rawrepoConnection, path, head);
        saveRelations(rawrepoConnection, head, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        // Volume
        saveMarcXChange(rawrepoConnection, path, volume);
        saveRelations(rawrepoConnection, volume, COMMON_AGENCY, Arrays.asList(
                new RecordId(head, COMMON_AGENCY),
                new RecordId(authority2, AUTHORITY_AGENCY)));

        // Control records
        saveMarcXChange(rawrepoConnection, path, control1);
        saveRelations(rawrepoConnection, control1, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        saveMarcXChange(rawrepoConnection, path, control2);
        saveRelations(rawrepoConnection, control2, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        saveMarcXChange(rawrepoConnection, path, control3);
        saveRelations(rawrepoConnection, control3, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        saveMarcXChange(rawrepoConnection, path, control4);
        saveRelations(rawrepoConnection, control4, COMMON_AGENCY, authority1, AUTHORITY_AGENCY);

        final Response response = callRecordService(volume, COMMON_ENRICHMENT, true, false);
        assertThat("Response code", response.getStatus(), is(200));

        final Map<String, RecordDTO> actual = response.readEntity(RecordDTOCollection.class).toMap();
        assertThat("actual size", actual.size(), is(6));

        assertThat("collection contains head", actual.containsKey(head), is(true));
        assertThat("collection content head", getMarcRecordFromString(actual.get(head).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, head))));
        assertThat("collection contains volume", actual.containsKey(volume), is(true));
        assertThat("collection content volume", getMarcRecordFromString(actual.get(volume).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, volume))));
        assertThat("collection contains control1", actual.containsKey(control1), is(true));
        assertThat("collection content control1", getMarcRecordFromString(actual.get(control1).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, control1))));
        assertThat("collection contains control2", actual.containsKey(control2), is(true));
        assertThat("collection content control2", getMarcRecordFromString(actual.get(control2).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, control2))));
        assertThat("collection contains control3", actual.containsKey(control3), is(true));
        assertThat("collection content control3", getMarcRecordFromString(actual.get(control3).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, control3))));
        assertThat("collection contains control4", actual.containsKey(control4), is(true));
        assertThat("collection content control4", getMarcRecordFromString(actual.get(control4).getContent()),
                is(getMarcRecordFromFile(String.format("%s/%s-expanded.xml", path, control4))));
    }

}
