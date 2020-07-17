/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.dto.EnqueueAgencyRequestDTO;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueRecordDTO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class QueueServiceIT extends AbstractRecordServiceContainerTest {

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

    @BeforeEach
    void resetQueue() {
        try {
            Connection rawrepoConnection = connectToRawrepoDb();
            resetRawrepoQueue(rawrepoConnection);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void enqueueAgencySameAgencyDefaultPriority() throws Exception {
        final EnqueueAgencyRequestDTO params = new EnqueueAgencyRequestDTO();
        params.setSelectAgencyId(870970);
        params.setWorker("socl-sync");

        final PathBuilder path = new PathBuilder("/api/v1/enqueue/agency");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(870970, 1500);
    }

    @Test
    void enqueueAgencyDifferentAgencyDefaultPriority() throws Exception {
        final EnqueueAgencyRequestDTO params = new EnqueueAgencyRequestDTO();
        params.setSelectAgencyId(870970);
        params.setEnqueueAgencyId(191919);
        params.setWorker("socl-sync");

        final PathBuilder path = new PathBuilder("/api/v1/enqueue/agency");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(191919, 1500);
    }

    @Test
    void enqueueAgencyDifferentAgencySpecificPriority() throws Exception {
        final EnqueueAgencyRequestDTO params = new EnqueueAgencyRequestDTO();
        params.setSelectAgencyId(870970);
        params.setEnqueueAgencyId(191919);
        params.setWorker("socl-sync");
        params.setPriority(42);

        final PathBuilder path = new PathBuilder("/api/v1/enqueue/agency");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(191919, 42);
    }

    private void assertEnqueueAgency(int agencyId, int priority) throws Exception{
        QueueJob queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue - got job", queueJob, notNullValue());
        assertThat("dequeue - correct record", queueJob.getJob(), is(new RecordId("50129691", agencyId)));
        assertThat("dequeue - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue - no more jobs", queueJob, nullValue());
    }

    @Test
    void enqueueRecordDefaultPriority() throws Exception {
        final EnqueueRecordDTO params = new EnqueueRecordDTO();
        params.setBibliographicRecordId("50129691");
        params.setAgencyId(870970);
        params.setChanged(true);
        params.setLeaf(true);
        params.setProvider("fbs-ph-update");

        final PathBuilder path = new PathBuilder("/api/v1/enqueue/record");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        assertEnqueueRecord(1000);
    }

    @Test
    void enqueueRecordSpecificPriority() throws Exception {
        final EnqueueRecordDTO params = new EnqueueRecordDTO();
        params.setBibliographicRecordId("50129691");
        params.setAgencyId(870970);
        params.setChanged(true);
        params.setLeaf(true);
        params.setProvider("fbs-ph-update");
        params.setPriority(42);

        final PathBuilder path = new PathBuilder("/api/v1/enqueue/record");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(path.build())
                .withData(params, MediaType.APPLICATION_JSON);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        assertEnqueueRecord(42);
    }

    private void assertEnqueueRecord(int priority) throws Exception {
        QueueJob queueJob = dequeue(connectToRawrepoDb(),"broend-sync");
        assertThat("dequeue broend-sync - got job", queueJob, notNullValue());
        assertThat("dequeue broend-sync - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue broend-sync - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"broend-sync");
        assertThat("dequeue broend-sync - no more jobs", queueJob, nullValue());

        queueJob = dequeue(connectToRawrepoDb(),"danbib-ph-libv3");
        assertThat("dequeue danbib-ph-libv3 - got job", queueJob, notNullValue());
        assertThat("dequeue danbib-ph-libv3 - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue danbib-ph-libv3 - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"danbib-ph-libv3");
        assertThat("dequeue danbib-ph-libv3 - no more jobs", queueJob, nullValue());

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue socl-sync - got job", queueJob, notNullValue());
        assertThat("dequeue socl-sync - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue socl-sync - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue socl-sync - no more jobs", queueJob, nullValue());
    }
}
