package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dump.AgencyParams;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DumpServiceFBSIT extends AbstractRecordServiceContainerTest {

    @BeforeAll
    static void initDB() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoDb(rawrepoConnection);

            saveRecord(rawrepoConnection, "sql/dump/agency-fbs/26006465-870970.xml", MIMETYPE_MARCXCHANGE);
            saveRecord(rawrepoConnection, "sql/dump/agency-fbs/26006465-191919.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/dump/agency-fbs/26006465-761500.xml", MIMETYPE_ENRICHMENT);
            saveRecord(rawrepoConnection, "sql/dump/agency-fbs/68774608-870979.xml", MIMETYPE_AUTHORITY);
            saveRecord(rawrepoConnection, "sql/dump/agency-fbs/68774608-191919.xml", MIMETYPE_ENRICHMENT);

            saveRelations(rawrepoConnection, "26006465", 191919, "26006465", 870970);
            saveRelations(rawrepoConnection, "26006465", 761500, "26006465", 870970);
            saveRelations(rawrepoConnection, "68774608", 191919, "68774608", 870979);
            saveRelations(rawrepoConnection, "26006465", 870970, "68774608", 870979);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void dumpAgency761500Expanded() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(761500));
        params.setMode("expanded");
        params.setRecordType(Collections.singletonList("ENRICHMENT"));
        params.setOutputFormat("XML");

        final PathBuilder path = new PathBuilder("/api/v1/dump");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final byte[] content = response.readEntity(byte[].class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-fbs/expected-dump-agencies.xml")));
    }

    @Test
    public void dumpRecords761500Expanded() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("mode", "expanded");
        params.put("output-format", "XML");

        String data = "26006465:761500";

        final PathBuilder path = new PathBuilder("/api/v1/dump/record");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(data, MediaType.TEXT_PLAIN);
        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final byte[] content = response.readEntity(byte[].class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-fbs/expected-dump-records.xml")));
    }
}
