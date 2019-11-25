package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.marc.binding.MarcRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RecordCollectionServiceIT extends AbstractRecordServiceContainerTest {


    @BeforeAll
    static void initDB() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoDb(rawrepoConnection);

            saveRecord(rawrepoConnection, "sql/collection/703213855-830380.xml", MIMETYPE_MARCXCHANGE);
            saveRecord(rawrepoConnection, "sql/collection/285927994-830380.xml", MIMETYPE_MARCXCHANGE);
            saveRelations(rawrepoConnection, "285927994", 830380, "703213855", 830380);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void getRecordCollection_DataIO_Ok() throws Exception {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/records/{agencyid}/{bibliographicrecordid}/content")
                        .bind("agencyid", "830380")
                        .bind("bibliographicrecordid", "285927994")
                        .bind("allow-deleted", "true")
                        .bind("use-parent-agency", "false")
                        .bind("exclude-aut-records", "true")
                        .bind("expand", "true")
                        .build());

        Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        String collection = response.readEntity(String.class);

        HashMap<String, MarcRecord> actual = getMarcRecordCollectionFromString(collection);
        assertThat("collection contains 285927994", actual.containsKey("285927994"), is(true));
        assertThat("collection content 285927994", actual.get("285927994"), is(getMarcRecordFromFile("sql/collection/285927994-830380.xml")));
        assertThat("collection contains 703213855", actual.containsKey("703213855"), is(true));
        assertThat("collection content 703213855", actual.get("703213855"), is(getMarcRecordFromFile("sql/collection/703213855-830380.xml")));
    }
}
