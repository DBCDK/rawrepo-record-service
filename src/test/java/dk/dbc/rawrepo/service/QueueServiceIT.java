package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

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
    void getQueueRules() {
        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/rules")
                        .build());

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final QueueRuleCollectionDTO queueRuleCollectionDTO = response.readEntity(QueueRuleCollectionDTO.class);
        assertThat("collection as rules", queueRuleCollectionDTO.getQueueRules(), notNullValue());
        assertThat("collection as size", queueRuleCollectionDTO.getQueueRules().size(), is(33));

        // Since there are 33 unique combinations and the collection is sorted we will only assert a few of the elements
        QueueRuleDTO queueRuleDTO = queueRuleCollectionDTO.getQueueRules().get(0);
        assertThat("queue rule 0 provider", queueRuleDTO.getProvider(), is("agency-delete"));
        assertThat("queue rule 0 worker", queueRuleDTO.getWorker(), is("broend-sync"));
        assertThat("queue rule 0 changed", queueRuleDTO.getChanged(), is('A'));
        assertThat("queue rule 0 leaf", queueRuleDTO.getLeaf(), is('Y'));
        assertThat("queue rule 0 description", queueRuleDTO.getDescription(), is("Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post"));

        queueRuleDTO = queueRuleCollectionDTO.getQueueRules().get(10);
        assertThat("queue rule 10 provider", queueRuleDTO.getProvider(), is("dataio-update"));
        assertThat("queue rule 10 worker", queueRuleDTO.getWorker(), is("oai-set-matcher"));
        assertThat("queue rule 10 changed", queueRuleDTO.getChanged(), is('A'));
        assertThat("queue rule 10 leaf", queueRuleDTO.getLeaf(), is('Y'));
        assertThat("queue rule 10 description", queueRuleDTO.getDescription(), is("Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post"));

        queueRuleDTO = queueRuleCollectionDTO.getQueueRules().get(30);
        assertThat("queue rule 30 provider", queueRuleDTO.getProvider(), is("solr-sync-bulk"));
        assertThat("queue rule 30 worker", queueRuleDTO.getWorker(), is("solr-sync-bulk"));
        assertThat("queue rule 30 changed", queueRuleDTO.getChanged(), is('Y'));
        assertThat("queue rule 30 leaf", queueRuleDTO.getLeaf(), is('N'));
        assertThat("queue rule 30 description", queueRuleDTO.getDescription(), is("Den rørte post, hvis det er en Hoved/Sektionsport"));
    }

    @Test
    void getQueueProviders() {
        final List<String> expected = Arrays.asList("agency-delete",
                "agency-maintain",
                "bulk-broend",
                "dataio-bulk",
                "dataio-ph-holding-update",
                "dataio-update",
                "dataio-update-well3.5",
                "fbs-ph-update",
                "fbs-update",
                "ims",
                "ims-bulk",
                "opencataloging-update",
                "solr-sync-bulk",
                "update-rawrepo-solr-sync");

        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/providers")
                        .build());

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final QueueProviderCollectionDTO actual = response.readEntity(QueueProviderCollectionDTO.class);
        assertThat("provider list", actual.getProviders(), is(expected));
    }

    @Test
    void getQueueWorkers() {
        final List<String> expected = Arrays.asList("basis-decentral",
                "broend-sync",
                "danbib-ph-libv3",
                "dataio-bulk-sync",
                "dataio-socl-sync-bulk",
                "ims-bulk-sync",
                "ims-sync",
                "oai-set-matcher",
                "socl-sync",
                "solr-sync-basis",
                "solr-sync-bulk");

        final HttpGet httpGet = new HttpGet(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/workers")
                        .build());

        final Response response = httpClient.execute(httpGet);
        assertThat("Response code", response.getStatus(), is(200));

        final QueueWorkerCollectionDTO actual = response.readEntity(QueueWorkerCollectionDTO.class);
        assertThat("worker list", actual.getWorkers(), is(expected));
    }

    @Test
    void enqueueAgencySameAgencyDefaultPriority() throws Exception {
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/{agencyid}/{worker}")
                        .bind("worker", "socl-sync")
                        .bind("agencyid", 870970).build());

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(870970, 1000);
    }

    @Test
    void enqueueAgencyDifferentAgencyDefaultPriority() throws Exception {
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/{agencyid}/{worker}")
                        .bind("agencyid", 870970)
                        .bind("worker", "socl-sync").build())
                .withQueryParameter("enqueue-as", 191919);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(191919, 1000);
    }

    @Test
    void enqueueAgencyDifferentAgencySpecificPriority() throws Exception {
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/{agencyid}/{worker}")
                        .bind("agencyid", 870970)
                        .bind("worker", "socl-sync").build())
                .withQueryParameter("enqueue-as", 191919)
                .withQueryParameter("priority", 42);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        final EnqueueAgencyResponseDTO responseDTO = response.readEntity(EnqueueAgencyResponseDTO.class);
        assertThat("get count", responseDTO.getCount(), is(1));

        assertEnqueueAgency(191919, 42);
    }

    private void assertEnqueueAgency(int agencyId, int priority) throws Exception{
        QueueJob queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue - got job", queueJob, CoreMatchers.notNullValue());
        assertThat("dequeue - correct record", queueJob.getJob(), is(new RecordId("50129691", agencyId)));
        assertThat("dequeue - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue - no more jobs", queueJob, nullValue());
    }

    @Test
    void enqueueRecordDefaultPriority() throws Exception {
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/{agencyid}/{bibliographicrecordid}/{provider}")
                        .bind("agencyid", 870970)
                        .bind("bibliographicrecordid", "50129691")
                        .bind("provider", "fbs-ph-update").build());

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        assertEnqueueRecord(1000);
    }

    @Test
    void enqueueRecordSpecificPriority() throws Exception {
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withPathElements(new PathBuilder("/api/v1/queue/{agencyid}/{bibliographicrecordid}/{provider}")
                        .bind("agencyid", 870970)
                        .bind("bibliographicrecordid", "50129691")
                        .bind("provider", "fbs-ph-update").build())
                .withQueryParameter("priority", 42);

        final Response response = httpClient.execute(httpPost);
        assertThat("Response code", response.getStatus(), is(200));

        assertEnqueueRecord(42);
    }

    private void assertEnqueueRecord(int priority) throws Exception {
        QueueJob queueJob = dequeue(connectToRawrepoDb(),"broend-sync");
        assertThat("dequeue broend-sync - got job", queueJob, CoreMatchers.notNullValue());
        assertThat("dequeue broend-sync - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue broend-sync - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"broend-sync");
        assertThat("dequeue broend-sync - no more jobs", queueJob, nullValue());

        queueJob = dequeue(connectToRawrepoDb(),"danbib-ph-libv3");
        assertThat("dequeue danbib-ph-libv3 - got job", queueJob, CoreMatchers.notNullValue());
        assertThat("dequeue danbib-ph-libv3 - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue danbib-ph-libv3 - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"danbib-ph-libv3");
        assertThat("dequeue danbib-ph-libv3 - no more jobs", queueJob, nullValue());

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue socl-sync - got job", queueJob, CoreMatchers.notNullValue());
        assertThat("dequeue socl-sync - correct record", queueJob.getJob(), is(new RecordId("50129691", 870970)));
        assertThat("dequeue socl-sync - correct priority", queueJob.getPriority(), is(priority));

        queueJob = dequeue(connectToRawrepoDb(),"socl-sync");
        assertThat("dequeue socl-sync - no more jobs", queueJob, nullValue());
    }
}
