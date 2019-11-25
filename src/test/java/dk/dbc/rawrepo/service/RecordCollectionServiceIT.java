package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.sql.Connection;

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
        // TODO Assert should be done against an expected collection and not a hardcoded string. The result is a list which may change order from compile to compile
        assertThat("get agencyId", collection, is("<?xml version='1.0' encoding='UTF-8'?>\n<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'><record><leader>00000n    2200000   4500</leader><datafield ind1='0' ind2='0' tag='001'><subfield code='a'>285927994</subfield><subfield code='b'>830380</subfield><subfield code='c'>20191121015709</subfield><subfield code='d'>20090630</subfield><subfield code='f'>a</subfield></datafield><datafield ind1='0' ind2='0' tag='004'><subfield code='r'>c</subfield><subfield code='a'>b</subfield><subfield code='n'>f</subfield></datafield><datafield ind1='0' ind2='0' tag='014'><subfield code='a'>703213855</subfield></datafield></record><record><leader>00000n    2200000   4500</leader><datafield ind1='0' ind2='0' tag='001'><subfield code='a'>703213855</subfield><subfield code='b'>830380</subfield><subfield code='c'>20191121015648</subfield><subfield code='d'>20090630</subfield><subfield code='f'>a</subfield></datafield><datafield ind1='0' ind2='0' tag='004'><subfield code='r'>c</subfield><subfield code='a'>h</subfield></datafield></record></collection>"));
    }
}
