/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dump.AgencyParams;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DumpServiceIT extends AbstractRecordServiceContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpServiceIT.class);

    @BeforeAll
    static void initDB() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoDb(rawrepoConnection);

            saveRecord(rawrepoConnection, "sql/dump/agency-dbc/common-marcxchange.xml", MIMETYPE_MARCXCHANGE);
            saveRecord(rawrepoConnection, "sql/dump/agency-dbc/common-enrichment.xml", MIMETYPE_ENRICHMENT);

            saveRecord(rawrepoConnection, "sql/dump/agency-dbc/authority-marcxchange.xml", MIMETYPE_AUTHORITY);
            saveRecord(rawrepoConnection, "sql/dump/agency-dbc/authority-enrichment.xml", MIMETYPE_ENRICHMENT);

            saveRelations(rawrepoConnection, "52557135", 191919, "52557135", 870970);
            saveRelations(rawrepoConnection, "69044638", 191919, "69044638", 870979);
            saveRelations(rawrepoConnection, "52557135", 870970, "69044638", 870979);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void dumpAgencyDBCRaw() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setMode("raw");
        params.setOutputFormat("XML");

        final PathBuilder path = new PathBuilder("/api/v1/dump");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-raw.xml")));
    }

    @Test
    public void dumpAgencyDBCMerged() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setMode("merged");
        params.setOutputFormat("XML");

        final PathBuilder path = new PathBuilder("/api/v1/dump");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-merged.xml")));
    }

    @Test
    public void dumpAgencyDBCExpanded() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setMode("expanded");
        params.setOutputFormat("XML");

        final PathBuilder path = new PathBuilder("/api/v1/dump");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-expanded.xml")));
    }

    @Test
    public void dumpRecordsDBCRaw() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("mode", "raw");
        params.put("output-format", "XML");

        String data = "52557135:870970";

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

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-raw.xml")));
    }

    @Test
    public void dumpRecordsDBCMerged() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("mode", "merged");
        params.put("output-format", "XML");

        String data = "52557135:870970";

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

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-merged.xml")));
    }

    @Test
    public void dumpRecordsDBCExpanded() throws Exception {
        final HashMap<String, Object> params = new HashMap<>();
        params.put("mode", "expanded");
        params.put("output-format", "XML");

        String data = "52557135:870970";

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

        String content = response.readEntity(String.class);

        assertThat("content", getMarcRecordFromString(content), CoreMatchers.is(getMarcRecordFromFile("sql/dump/agency-dbc/expected-expanded.xml")));
    }

}
