/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dao.HoldingsItemsBean;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class BibliographicIdResultSetTest {

    private final HashMap<String, String> rawrepoRecordIdsFor710100 = new HashMap<String, String>() {{
        put("A", "text/marcxchange");
        put("B", "text/marcxchange");
        put("C", "text/enrichment+marcxchange");
        put("D", "text/enrichment+marcxchange");
    }};

    private final HashMap<String, String> holdingsRecordIdsFor710100 = new HashMap<String, String>() {{
        put("B", "holdings");
        put("D", "holdings");
        put("E", "holdings");
    }};

    private final HashMap<String, String> rawrepoRecordIdsFor870970 = new HashMap<String, String>() {{
        put("A", "text/marcxchange");
        put("B", "text/marcxchange");
        put("C", "text/marcxchange");
        put("D", "text/marcxchange");
        put("E", "text/marcxchange");
    }};

    @Mock
    private RawRepoBean rawRepoBean;

    @Mock
    private HoldingsItemsBean holdingsItemsBean;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNormalList() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 2, rawrepoRecordIdsFor870970, null);

        Assert.assertThat(resultSet.size(), is(5));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testListSmallerThanSliceSize() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 10, rawrepoRecordIdsFor870970, null);

        Assert.assertThat(resultSet.size(), is(5));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testListSizeEqualsSliceSize() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 5, rawrepoRecordIdsFor870970, null);

        Assert.assertThat(resultSet.size(), is(5));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testEmptyList() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 2, new HashMap<>(), null);

        Assert.assertThat(resultSet.size(), is(0));
    }

    @Test
    public void testFBSAllRecordTypes() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        Assert.assertThat(resultSet.size(), is(5));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/enrichment+marcxchange");
            put("D", "text/enrichment+marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFBSLocalAndEnrichments() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, null);

        Assert.assertThat(resultSet.size(), is(4));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/enrichment+marcxchange");
            put("D", "text/enrichment+marcxchange");
        }}));
    }

    @Test
    public void testFSBLocalAndHoldings() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        Assert.assertThat(resultSet.size(), is(4));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFSBEnrichmentAndHoldings() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        Assert.assertThat(resultSet.size(), is(4));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("B", "text/marcxchange");
            put("C", "text/enrichment+marcxchange");
        }}));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFSBHoldingsOnly() throws Exception {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 3, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        Assert.assertThat(resultSet.size(), is(3));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("B", "text/marcxchange");
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testRecordParams() throws Exception {
        RecordParams params = new RecordParams();

        List<RecordIdDTO> recordIdDTOs = new ArrayList<>();
        recordIdDTOs.add(new RecordIdDTO("1111", 191919));
        recordIdDTOs.add(new RecordIdDTO("2222", 191919));
        recordIdDTOs.add(new RecordIdDTO("3333", 723000));
        params.setRecordIds(recordIdDTOs);

        Assert.assertThat(params.getAgencies().size(), is(2));
        Assert.assertTrue(params.getAgencies().contains(191919));
        Assert.assertTrue(params.getAgencies().contains(723000));

        Assert.assertThat(params.getBibliographicRecordIdByAgencyId(191919).size(), is(2));
        Assert.assertTrue(params.getBibliographicRecordIdByAgencyId(191919).contains("1111"));
        Assert.assertTrue(params.getBibliographicRecordIdByAgencyId(191919).contains("2222"));

        Assert.assertThat(params.getBibliographicRecordIdByAgencyId(723000).size(), is(1));
        Assert.assertTrue(params.getBibliographicRecordIdByAgencyId(723000).contains("3333"));
    }

    @Test
    public void testRecordParamsBibliographicIdResultSet() throws Exception {
        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(3, rawrepoRecordIdsFor870970);

        Assert.assertThat(resultSet.size(), is(5));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
        }}));

        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

}
