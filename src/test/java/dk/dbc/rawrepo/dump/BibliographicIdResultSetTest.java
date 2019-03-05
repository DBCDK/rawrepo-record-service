package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dao.HoldingsItemsBean;
import dk.dbc.rawrepo.dao.RawRepoBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

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
        Params params = new Params();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(870970), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor870970);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(870970, AgencyType.DBC, params, 2, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(870970), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor870970);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(870970, AgencyType.DBC, params, 10, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(870970), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor870970);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(870970, AgencyType.DBC, params, 5, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setAgencies(Arrays.asList(870970));
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(870970), eq(RecordStatus.ALL))).thenReturn(new HashMap<>());

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(870970, AgencyType.DBC, params, 2, rawRepoBean, holdingsItemsBean);

        Assert.assertThat(resultSet.size(), is(0));
    }

    @Test
    public void testFBSAllRecordTypes() throws Exception {
        Params params = new Params();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(710100), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor710100);
        when(holdingsItemsBean.getRecordIdsWithHolding(eq(710100))).thenReturn(holdingsRecordIdsFor710100);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(710100, AgencyType.FBS, params, 2, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(710100), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor710100);
        when(holdingsItemsBean.getRecordIdsWithHolding(eq(710100))).thenReturn(holdingsRecordIdsFor710100);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(710100, AgencyType.FBS, params, 2, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.LOCAL.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(710100), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor710100);
        when(holdingsItemsBean.getRecordIdsWithHolding(eq(710100))).thenReturn(holdingsRecordIdsFor710100);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(710100, AgencyType.FBS, params, 2, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.ENRICHMENT.toString());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(710100), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor710100);
        when(holdingsItemsBean.getRecordIdsWithHolding(eq(710100))).thenReturn(holdingsRecordIdsFor710100);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(710100, AgencyType.FBS, params, 2, rawRepoBean, holdingsItemsBean);

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
        Params params = new Params();
        params.setRecordType(new ArrayList<>());
        params.getRecordType().add(RecordType.HOLDINGS.toString());
        params.setRecordStatus(RecordStatus.ALL.toString());

        when(rawRepoBean.getBibliographicRecordIdForAgency(eq(710100), eq(RecordStatus.ALL))).thenReturn(rawrepoRecordIdsFor710100);
        when(holdingsItemsBean.getRecordIdsWithHolding(eq(710100))).thenReturn(holdingsRecordIdsFor710100);

        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(710100, AgencyType.FBS, params, 3, rawRepoBean, holdingsItemsBean);

        Assert.assertThat(resultSet.size(), is(3));
        Assert.assertThat(resultSet.next(), is(new HashMap<String, String>() {{
            put("B", "text/marcxchange");
            put("D", "text/enrichment+marcxchange");
            put("E", "holdings");
        }}));
    }
}
