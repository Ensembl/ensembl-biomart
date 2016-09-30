package org.ensembl.mart.lib.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.Query;

/**
 * Tests that Mart Explorer Sequence retrieval works by comparing it's output to that of ensj.
 * 
 * @author craig
 *
 */
public class AttributeTest extends Base {

	public static void main(String[] args) {
		if (args.length > 0)
			TestRunner.run(TestClass(args[0]));
		else
			TestRunner.run(suite());
	}

	public static Test suite() {
		TestSuite suite = new TestSuite();
		//suite.addTest(new AttributeTest("testKakaQuery"));
		suite.addTestSuite( AttributeTest.class );
		return suite;
	}

	public static Test TestClass(String testclass) {
		TestSuite suite = new TestSuite();
		suite.addTest(new AttributeTest(testclass));
		return suite;
	}

	public AttributeTest(String name) {
		super(name);
	}

	public void testKakaQuery()  throws Exception {
		Query q = new Query(genequery);
		
		q.addAttribute(new FieldAttribute("chr_name","main","gene_id_key"));
		q.addAttribute(new FieldAttribute("gene_chrom_start","main","gene_id_key"));
		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
		q.addFilter(new BasicFilter("gene_chrom_start","main","gene_id_key", "<", "15000000"));
    

		StatOutputStream stats = new StatOutputStream();
		engine.execute(q, new FormatSpec(FormatSpec.TABULATED), stats);

		assertTrue("No text returned from query", stats.getCharCount() > 0);
		assertTrue("No lines returned from query", stats.getLineCount() > 0);
		stats.close();
	}

	public void testSimpleQueries() throws Exception {
		Query q = new Query(genequery);
		
		q.setPrimaryKeys(new String[] { "gene_id_key", "transcript_id_key" });
		q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
		q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
		StatOutputStream stats = new StatOutputStream();
		engine.execute(q, new FormatSpec(FormatSpec.TABULATED), stats);

		assertTrue("No text returned from query", stats.getCharCount() > 0);
		assertTrue("No lines returned from query", stats.getLineCount() > 0);
		stats.close();
	}

	public void testSimpleSNPQueries() throws Exception {
		Query q = new Query(snpquery);

		q.addAttribute(new FieldAttribute("external_id","main","snp_id_key"));
		q.addAttribute(new FieldAttribute("allele","main","snp_id_key"));
		q.addFilter(new BasicFilter("chr_name","main","snp_id_key", "=", "21"));
		StatOutputStream stats = new StatOutputStream();
		engine.execute(q, new FormatSpec(FormatSpec.TABULATED), stats);
		assertTrue("No text returned from query", stats.getCharCount() > 0);
		assertTrue("No lines returned from query", stats.getLineCount() > 0);
		stats.close();
	}
  
//TODO: need better documentation on the need for this test
//	public void testDisambiguationQueries() throws Exception {
//		String geneID = "ENSG00000079974";
//		String expectedDiseaseID = "RB2B_HUMAN";
//		Query q = new Query(genequery);
//		
//		q.addAttribute(new FieldAttribute("display_id_list", "hsapiens_gene_ensembl__xref_uniprot_swissprot__dm", "transcript_id_key"));
//		q.addFilter(new BasicFilter("gene_stable_id", "main", "gene_id_key","=", geneID));
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		engine.execute(q, new FormatSpec(FormatSpec.TABULATED), out);
//		out.close();
//		String actualDiseaseID = out.toString().trim();
//		assertEquals(
//			"Got wrong disease ID for gene " + geneID,
//			expectedDiseaseID,
//			actualDiseaseID);
//	}
}
