package org.ensembl.mart.lib.test;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.QueryListener;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Tests Query's setters, getters and listener notification mechanisms.
 * 
 * @author craig
 *
 */
public class QueryTest extends TestCase implements QueryListener {

  private Query q = null;
  private int numChanges = 0;
  private int expectedNumChanges = 0;

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    //suite.addTest(new QueryTest("testKakaQuery"));
    suite.addTestSuite(QueryTest.class);
    return suite;
  }

  public QueryTest(String name) {
    super(name);
  }

  public void setUp() {
    q = new Query();
    q.addQueryChangeListener(this);
    numChanges = 0;
    expectedNumChanges = 0;
  }

  public void testSettingPrimaryKeys() throws Exception {

    String[] pKeys = new String[] { "gene_id", "transcript_id" };
    q.setPrimaryKeys(pKeys);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(pKeys, q.getPrimaryKeys());
    pKeys = null;
    q.setPrimaryKeys(pKeys);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(pKeys, q.getPrimaryKeys());

  }

  public void testSettingAttributes() throws Exception {

    Attribute a = new FieldAttribute("chr_name");
    Attribute a2 = new FieldAttribute("geneName");
    Attribute a3 = new FieldAttribute("geneLength");

    q.addAttribute(a);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(a, q.getAttributes()[0]);

    q.removeAttribute(a);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(0, q.getAttributes().length);

    q.addAttribute(a);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    try {

      q.addAttribute(a);
      fail("Shouldn't add attribute if already included");
    } catch (IllegalArgumentException e) {
    }

    q.addAttribute(a2);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(a2, q.getAttributes()[1]);

    q.addAttribute(1, a3);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);

    assertEquals(3, q.getAttributes().length);
    assertEquals(a, q.getAttributes()[0]);
    assertEquals(a3, q.getAttributes()[1]);
    assertEquals(a2, q.getAttributes()[2]);

    q.removeAttribute(a3);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(a, q.getAttributes()[0]);
    assertEquals(a2, q.getAttributes()[1]);
  }

  public void testSettingFilters() throws Exception {

    Filter f = new BasicFilter("chr_name", "=", "22");
    Filter f2 = new BasicFilter("geneName", "=", "fred");
    Filter f3 = new BasicFilter("numDiseases", ">", "3");

    q.addFilter(f);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(f, q.getFilters()[0]);

    q.addFilter(f2);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(f, q.getFilters()[0]);
    assertEquals(f2, q.getFilters()[1]);

    q.addFilter(1, f3);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(f, q.getFilters()[0]);
    assertEquals(f3, q.getFilters()[1]);
    assertEquals(f2, q.getFilters()[2]);

    q.removeFilter(f);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(2, q.getFilters().length);
    assertEquals(f3, q.getFilters()[0]);
    assertEquals(f2, q.getFilters()[1]);

    q.replaceFilter(f2, f);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(2, q.getFilters().length);
    assertEquals(f3, q.getFilters()[0]);
    assertEquals(f, q.getFilters()[1]);

    q.addFilter(f2);
    expectedNumChanges++;

    q.removeFilter(f3);
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
    assertEquals(f, q.getFilters()[0]);
    assertEquals(f2, q.getFilters()[1]);

  }

  public void testSettingLimit() throws Exception {
    int limit = 100;
    q.setLimit(limit);
    assertEquals(limit, q.getLimit());
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);

  }

  public void testSettingDatasetInternalName() throws Exception {
    String datasetInternalName = "DS1";
    q.setDataset(datasetInternalName);
    assertEquals(datasetInternalName, q.getDataset());
    expectedNumChanges++;
    assertEquals(expectedNumChanges, numChanges);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryDatasetInternalNameChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void datasetChanged(
    Query sourceQuery,
    String oldDatasetInternalName,
    String newDatasetInternalName) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryStarBasesChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void starBasesChanged(
    Query sourceQuery,
    String[] oldStarBases,
    String[] newStarBases) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryPrimaryKeysChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void primaryKeysChanged(
    Query sourceQuery,
    String[] oldPrimaryKeys,
    String[] newPrimaryKeys) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryNameChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void queryNameChanged(
    Query sourceQuery,
    String oldName,
    String newName) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryDatasourceChanged(org.ensembl.mart.lib.Query, javax.sql.DataSource, javax.sql.DataSource)
   */
  public void datasourceChanged(
    Query sourceQuery,
    DataSource oldDatasource,
    DataSource newDatasource) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryAttributeAdded(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Attribute)
   */
  public void attributeAdded(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryAttributeRemoved(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Attribute)
   */
  public void attributeRemoved(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryFilterAdded(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter)
   */
  public void filterAdded(Query sourceQuery, int index, Filter filter) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryFilterRemoved(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter)
   */
  public void filterRemoved(Query sourceQuery, int index, Filter filter) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#querySequenceDescriptionChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.SequenceDescription, org.ensembl.mart.lib.SequenceDescription)
   */
  public void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription newSequenceDescription) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryLimitChanged(org.ensembl.mart.lib.Query, int, int)
   */
  public void limitChanged(Query query, int oldLimit, int newLimit) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryFilterChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter, org.ensembl.mart.lib.Filter)
   */
  public void filterChanged(
    Query sourceQuery,
    int index,
    Filter oldFilter,
    Filter newFilter) {
    numChanges++;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#datasetConfigChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.config.DatasetConfig, org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig) {
    numChanges++;
  }

}
