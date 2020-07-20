package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

public class QueueServiceIT extends AbstractRecordServiceContainerTest {

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
}
