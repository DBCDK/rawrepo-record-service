package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dto.RecordIdDTO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNormalList() {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 2, rawrepoRecordIdsFor870970, null);

        assertThat(resultSet.size(), is(5));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testListSmallerThanSliceSize() {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 10, rawrepoRecordIdsFor870970, null);

        assertThat(resultSet.size(), is(5));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testListSizeEqualsSliceSize() {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 5, rawrepoRecordIdsFor870970, null);

        assertThat(resultSet.size(), is(5));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

    @Test
    public void testEmptyList() {
        AgencyParams params = new AgencyParams();
        params.setAgencies(Collections.singletonList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.DBC, 2, new HashMap<>(), null);

        assertThat(resultSet.size(), is(0));
    }

    @Test
    public void testFBSAllRecordTypes() {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        assertThat(resultSet.size(), is(5));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/enrichment+marcxchange");
            put("D", "text/enrichment+marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFBSLocalAndEnrichments() {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, null);

        assertThat(resultSet.size(), is(4));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("C", "text/enrichment+marcxchange");
            put("D", "text/enrichment+marcxchange");
        }}));
    }

    @Test
    public void testFSBLocalAndHoldings() {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        assertThat(resultSet.size(), is(4));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFSBEnrichmentAndHoldings() {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 2, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        assertThat(resultSet.size(), is(4));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("B", "text/marcxchange");
            put("C", "text/enrichment+marcxchange");
        }}));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testFSBHoldingsOnly() {
        AgencyParams params = new AgencyParams();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(params, AgencyType.FBS, 3, rawrepoRecordIdsFor710100, holdingsRecordIdsFor710100);

        assertThat(resultSet.size(), is(3));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("B", "text/marcxchange");
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }

    @Test
    public void testRecordParams() {
        RecordParams params = new RecordParams();

        List<RecordIdDTO> recordIdDTOs = new ArrayList<>();
        recordIdDTOs.add(new RecordIdDTO("1111", 191919));
        recordIdDTOs.add(new RecordIdDTO("2222", 191919));
        recordIdDTOs.add(new RecordIdDTO("3333", 723000));
        params.setRecordIds(recordIdDTOs);

        assertThat(params.getAgencies().size(), is(2));
        assertThat(params.getAgencies().contains(191919), is(true));
        assertThat(params.getAgencies().contains(723000), is(true));

        assertThat(params.getBibliographicRecordIdByAgencyId(191919).size(), is(2));
        assertThat(params.getBibliographicRecordIdByAgencyId(191919).contains("1111"), is(true));
        assertThat(params.getBibliographicRecordIdByAgencyId(191919).contains("2222"), is(true));

        assertThat(params.getBibliographicRecordIdByAgencyId(723000).size(), is(1));
        assertThat(params.getBibliographicRecordIdByAgencyId(723000).contains("3333"), is(true));
    }

    @Test
    public void testRecordParamsBibliographicIdResultSet() {
        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(3, rawrepoRecordIdsFor870970);

        assertThat(resultSet.size(), is(5));
        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("A", "text/marcxchange");
            put("B", "text/marcxchange");
            put("C", "text/marcxchange");
        }}));

        assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("D", "text/marcxchange");
            put("E", "text/marcxchange");
        }}));
    }

}
