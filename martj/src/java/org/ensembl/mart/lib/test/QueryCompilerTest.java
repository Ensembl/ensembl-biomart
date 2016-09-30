package org.ensembl.mart.lib.test;

import java.io.File;
import java.net.URL;
import java.util.logging.Logger;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.IDListFilter;
import org.ensembl.mart.lib.Query;
import org.ensembl.util.PropertiesUtil;

/** JUnit TestSuite. 
 * @testfamily JUnit
 * @testkind testsuite
 * @testsetup Default TestSuite
 * @testpackage org.ensembl.mart.explorer.test*/
public class QueryCompilerTest extends Base {

  private Logger logger =
		Logger.getLogger(QueryCompilerTest.class.getName());

	public final String STABLE_ID_REL = "data/unitTests/gene_stable_id.test";
	private StatOutputStream stats = new StatOutputStream();
	private FormatSpec formatspec = new FormatSpec(FormatSpec.TABULATED, "\t");

	public QueryCompilerTest(String name) {
		super(name);
	}

	public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(QueryCompilerTest.class);
		return suite;
	}

	public static Test TestClass(String testclass) {
		TestSuite suite = new TestSuite();
		suite.addTest(new QueryCompilerTest(testclass));
		return suite;
	}

	/**
	 * Convenience method for executing query and printing some results.
	 */
	private void executeQuery(Query query) throws Exception {
		engine.execute(query, formatspec, stats);
//		System.out.println(query);
//		System.out.println(stats);

		assertTrue("No text returned from query", stats.getCharCount() > 0);
		assertTrue("No lines returned from query", stats.getLineCount() > 0);
	}

	public void testQueryCopy() throws Exception {
		Query q = new Query(genequery);
		assertTrue("Query Copy Constructor creating a equal copy\n", genequery.equals(q));
	}

	public void testChrQuery() throws Exception {
		Query q = new Query(genequery);

		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "3"));

		executeQuery(q);
	}

	public void testStableIDQuery() throws Exception {
		Query q = new Query(genequery);

		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", new String[] { "ENSG00000005175" }));
		executeQuery(q);
	}

	/**
	 * Test filtering on stable ids from a file.
	 */
	public void testStableIDsFromFileQuery() throws Exception {
		Query q = new Query(genequery);

		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", new File(PropertiesUtil.class.getClassLoader().getResource(STABLE_ID_REL).getFile())));
		executeQuery(q);
	}

	public void testStableIDsFromURLQuery() throws Exception {

		// in practice this is effectively the same as testStableIDsFromFile because
		// the implementation converts the file to a url. We include this test incase future
		// implementations work differently.
		Query q = new Query(genequery);

		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));

		URL stableidurl = QueryCompilerTest.class.getClassLoader().getResource(STABLE_ID_REL);
		q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", stableidurl));
		executeQuery(q);
	}

	public void testJoinToPFAM() throws Exception {
		Query q = new Query(genequery);

		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addAttribute(new FieldAttribute("pfam_bool","main","gene_id_key"));
		executeQuery(q);
	}

	public void testUnprocessedFilterHandlers() throws Exception {
		Filter chrFilter = new BasicFilter("chr_name", "main",  "gene_id_key", "=", "1");

        // Generic Handler relies on a DatasetConfig so below tests can't work anymore

//		//Marker
//		Query q = new Query(genequery);
//		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
//		q.addFilter(chrFilter);
//
//		Filter start = new BasicFilter("glook_marker_start","hsapiens__marker__look",null, "=", "AFMA272XC9", "org.ensembl.mart.lib.GenericHandler");
//	  Filter end = new BasicFilter("glook_marker_end","hsapiens__marker__look",null, "=", "RH10794", "org.ensembl.mart.lib.GenericHandler");
//		
//		q.addFilter(start);
//		q.addFilter(end);
//
//		executeQuery(q);
//
//		//Band
//		q = new Query(genequery);
//		q.addAttribute(new FieldAttribute("gene_stable_id", "main","gene_id_key"));
//		q.addFilter(chrFilter);
//		
//		start = new BasicFilter("glook_band_start","hsapiens__karotype__look",null, "=", "p36.33", "org.ensembl.mart.lib.GenericHandler");
//		end = new BasicFilter("glook_band_end","hsapiens__karotype__look",null, "=", "p36.33", "org.ensembl.mart.lib.GenericHandler");
//
//		q.addFilter(start);
//		q.addFilter(end);
//		executeQuery(q);
//
////		//Encode
//		q = new Query(genequery);
//
//		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
//
//		Filter test = new BasicFilter("glook_encode_region","main","gene_id_key", "=", "13:29450016:29950015", "org.ensembl.mart.lib.GenericHandler");
//
//		q.addFilter(test);
//		executeQuery(q);
////
////		//Qtl
//		q = new Query(genequery);
//		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
//
//		test = new BasicFilter("qtl","main","gene_id_key", "=", "4:82189556:83189556", "org.ensembl.mart.lib.GenericHandler");
//
//		q.addFilter(test);
//		executeQuery(q);

		//Expression
		Query q = new Query(genequery);
		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));

		Filter anatomical_filter = new BasicFilter("est.anatomical_site","main","gene_id_key", "=", "ovary");
		Filter development_filter = new BasicFilter("est.development_stage","main","gene_id_key", "=", "adult");
		
		q.addFilter(anatomical_filter);
		q.addFilter(development_filter);
		executeQuery(q);
		
		//GO
		//q = new Query(genequery);
		//q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));

        //Filter evidence_code = new BasicFilter("go_evidence_code:IEA","main","gene_id_key", "only", null, "org.ensembl.mart.lib.GOFilterHandler");
		//Filter mol_function_filter = new BasicFilter("go_molecular_function","main","gene_id_key", "=", "GO:0003673", "org.ensembl.mart.lib.GOFilterHandler");
		
		//q.addFilter(evidence_code);
		//q.addFilter(mol_function_filter);
		
		//executeQuery(q);
		//TODO:GO
		/*
    mol Function          GO:0003673
    biol proc             GO:0008150
    cell comp             GO:0005575
    go_evidence_code:IEA  excluded
  
    5345 entries
     
    ======  
    mol Function          chaperone activity
    biol proc             development
    cell comp             cell
    go_evidence_code:IEA  excluded

  3 entries
		 */
	}

	public static void main(String[] args) {
		if (args.length > 0)
			junit.textui.TestRunner.run(TestClass(args[0]));
		else
			junit.textui.TestRunner.run(suite());
	}
}
