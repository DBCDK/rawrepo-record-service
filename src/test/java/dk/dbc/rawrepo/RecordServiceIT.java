/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.commons.jdbc.util.JDBCUtil;
import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RecordServiceIT extends AbstractRecordServiceContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRecordServiceContainerTest.class);

    private final static URL rawrepoDumpResource = RecordServiceIT.class.getResource("schema/rawrepo.sql");
    private final static URL queueRulesDumpResource = RecordServiceIT.class.getResource("schema/queuerules.sql");
    //    private final static URL holdingsItemsDumpResource = RecordServiceIT.class.getResource("schema/holdingsitems.sql");
    // TODO Initialize holdings items db

    @BeforeAll
    static void initDB() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            JDBCUtil.executeScript(rawrepoConnection,
                    new File(rawrepoDumpResource.toURI()), StandardCharsets.UTF_8.name());
            JDBCUtil.executeScript(rawrepoConnection,
                    new File(queueRulesDumpResource.toURI()), StandardCharsets.UTF_8.name());

            Connection holdingsItemsConnection = connectToHoldingsItemsDb();
//            JDBCUtil.executeScript(holdingsItemsConnection,
//                    new File(holdingsItemsConnection.toURI()), StandardCharsets.UTF_8.name());
        } catch (IOException | URISyntaxException | SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void test1() throws Exception {
        Connection connection = connectToRawrepoDb();
        String content = fileToContent("sql/50129691-191919.xml");

        insertRecord(connection, "50129691", 191919, "enrichment", content.getBytes());
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/record/{agencyId}/{bibliographicRecordId}/exists")
                        .bind("bibliographicRecordId", 50129691)
                        .bind("agencyId", 191919)
                        .build());

        Response response = httpClient.execute(httpGet);
        RecordExistsDTO recordExistsDTO = response.readEntity(RecordExistsDTO.class);

        assertThat("getApplicants-1 in response", recordExistsDTO.isValue(), is(true));
    }
}
