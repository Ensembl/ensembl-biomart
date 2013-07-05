/*
    Copyright (C) 2003 EBI, GRL

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
 
package org.ensembl.mart.shell.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.test.Base;
import org.ensembl.mart.lib.test.StatOutputStream;
import org.ensembl.mart.shell.MartShellLib;

/**
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class MartShellLibTest extends Base {

  private MartShellLib msl;
  
	public static void main(String[] args) {
		if (args.length > 0)
			TestRunner.run(TestClass(args[0]));
		else
			TestRunner.run(suite());
	}

	public static Test suite() {
		return new TestSuite(MartShellLibTest.class);
	}

	public static Test TestClass(String testclass) {
		TestSuite suite = new TestSuite();
		suite.addTest(new MartShellLibTest(testclass));
		return suite;
	}

	public MartShellLibTest(String name) {
		super(name);
	}

  public void testMQLtoQuery() throws Exception {
  	String martSQL = "using hsapiens_gene_ensembl get gene_stable_id limit 100";
		StatOutputStream stats = new StatOutputStream();
    
    Query query = msl.MQLtoQuery(martSQL);
    query.setDataSource(martJDataSource);
        
    engine.execute(query, FormatSpec.TABSEPARATEDFORMAT, stats);
    
    int charCount = stats.getCharCount();
    int lineCount = stats.getLineCount();
		assertTrue("No text returned from query\n", charCount > 0);
		assertTrue("No lines returned from query\n", lineCount > 0);
		assertEquals("Wrong number of genes returned from Query\n", 100, lineCount);
		
		stats.close();		
  }

//TODO: when QueryToMQL works, uncomment this  
//  public void testQueryToMQL() throws Exception {
//    msl.setEnvDataset("ensembl_genes_homo_sapiens");
//    
//    String test = "get ensembl_gene_id";
//    Query testQuery = msl.MQLtoQuery(test);
//    String response = msl.QueryToMQL(testQuery);
//    assertEquals("using ensembl_genes_homo_sapiens " + test, response);
//    
//    test = "get ensembl_gene_id limit 10";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals("using ensembl_genes_homo_sapiens " + test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id, ensembl_transcript_id, ensembl_translation_id";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where disease_genes only";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where disease_genes excluded";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where chromosome_name = 2";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where chromosome_name in (1, 2)";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where chromosome_name in data/exampleChromosomeList";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where chromosome_name in file:data/exampleChromosomeList";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    String subMQL = "using ensembl_genes_mus_musculus get human_homologue_ensembl_gene_id where transmembrane_domains only as MouseHumanTrans";
//    String subMQLQ = "using ensembl_genes_mus_musculus get human_homologue_ensembl_gene_id where transmembrane_domains only";
//    String subMQLK = "MouseHumanTrans";
//    
//    msl.addStoredMQLCommand(subMQLK, subMQLQ);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where ensembl_gene_id in MouseHumanTrans";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(subMQL + ";" + test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get ensembl_gene_id where ensembl_gene_id in MouseHumanTrans and disease_genes only and snp_ka_ks_ratio > 0.1";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(subMQL + ";" + test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence peptide where disease_genes only";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//       
//    test = "using ensembl_genes_homo_sapiens get ensembl_transcript_id sequence 1000+gene_exons+1000 where disease_genes only";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    //test all sequences
//    test = "using ensembl_genes_homo_sapiens get sequence coding";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence peptide";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence cdna";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence transcript_exons";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence transcript_exon_intron";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence 1000+transcript_flanks";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence gene_exon_intron";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence gene_exons";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence gene_flanks+1000";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence downstream_utr+1000";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//    
//    test = "using ensembl_genes_homo_sapiens get sequence 1000+upstream_utr";
//    testQuery = msl.MQLtoQuery(test);
//    response = msl.QueryToMQL(testQuery);
//    assertEquals(test, response);
//  }
    
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	public void setUp() throws Exception {
		super.setUp();

    msl = new MartShellLib();
    
    msl.addMart(martJDataSource);
    msl.setEnvMart(martJDataSource.getName());
	}
}
