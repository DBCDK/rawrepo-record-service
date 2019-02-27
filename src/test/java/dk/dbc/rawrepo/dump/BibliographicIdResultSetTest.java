package dk.dbc.rawrepo.dump;

public class BibliographicIdResultSetTest {

//    @Mock
//    private RawRepoBean bean;
//
//    @Before
//    public void setUp() {
//        MockitoAnnotations.initMocks(this);
//    }
//
//    @Test
//    public void testNormalList() throws Exception {
//        Params params = new Params();
//        params.setAgencies(Arrays.asList(870970));
//        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(bean, 2, params);
//        List<String> res = Arrays.asList("A", "B", "C", "D", "E");
//
//        when(bean.getBibliographicRecordIdsForEnrichmentAgency(eq(870970), eq(191919))).thenReturn(res);
//
//        resultSet.fetchRecordIds(870970, AgencyType.DBC);
//        Assert.assertThat(resultSet.size(), is(5));
//        Assert.assertThat(resultSet.next(), is(Arrays.asList("A", "B")));
//        Assert.assertThat(resultSet.next(), is(Arrays.asList("C", "D")));
//        Assert.assertThat(resultSet.next(), is(Arrays.asList("E")));
//        Assert.assertThat(resultSet.next(), is(nullValue()));
//    }
//
//
//    @Test
//    public void testListSmallerThanSliceSize() throws Exception {
//        Params params = new Params();
//        params.setAgencies(Arrays.asList(870970));
//
//        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(bean, 2, params);
//        List<String> res = Arrays.asList("A");
//
//        when(bean.getBibliographicRecordIdsForEnrichmentAgency(eq(870970), eq(191919))).thenReturn(res);
//
//        resultSet.fetchRecordIds(870970, AgencyType.DBC);
//        Assert.assertThat(resultSet.size(), is(1));
//        Assert.assertThat(resultSet.next(), is(Arrays.asList("A")));
//        Assert.assertThat(resultSet.next(), is(nullValue()));
//    }
//
//    @Test
//    public void testListSizeEqualsSliceSize() throws Exception {
//        Params params = new Params();
//        params.setAgencies(Arrays.asList(870970));
//
//        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(bean, 2, params);
//        List<String> res = Arrays.asList("A", "B");
//
//        when(bean.getBibliographicRecordIdsForEnrichmentAgency(eq(870970), eq(191919))).thenReturn(res);
//
//        resultSet.fetchRecordIds(870970, AgencyType.DBC);
//        Assert.assertThat(resultSet.size(), is(2));
//        Assert.assertThat(resultSet.next(), is(Arrays.asList("A", "B")));
//        Assert.assertThat(resultSet.next(), is(nullValue()));
//    }
//
//    @Test
//    public void testEmptyList() throws Exception {
//        Params params = new Params();
//        params.setAgencies(Arrays.asList(870970));
//
//        BibliographicIdResultSet resultSet = new BibliographicIdResultSet(bean, 2, params);
//        List<String> res = Arrays.asList();
//
//        when(bean.getBibliographicRecordIdsForEnrichmentAgency(eq(870970), eq(191919))).thenReturn(res);
//
//        resultSet.fetchRecordIds(870970, AgencyType.DBC);
//        Assert.assertThat(resultSet.size(), is(0));
//        Assert.assertThat(resultSet.next(), is(nullValue()));
//    }
}
