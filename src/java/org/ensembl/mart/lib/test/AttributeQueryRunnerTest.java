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
package org.ensembl.mart.lib.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.BooleanFilter;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.IDListFilter;
import org.ensembl.mart.lib.Query;

/** 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public class AttributeQueryRunnerTest extends Base {

  private final int NO_HARD_LIMIT = 0;
  private final int SMALL_HARD_LIMIT = 100;
  private final int BIG_HARD_LIMIT = 200000;
  
  /**
   * @param name
   */
  public AttributeQueryRunnerTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    if (args.length > 0)
      TestRunner.run(TestClass(args[0]));
    else
      TestRunner.run(suite());
  }

  public static Test TestClass(String testclass) {
    TestSuite suite = new TestSuite();
    suite.addTest(new AttributeQueryRunnerTest(testclass));
    return suite;
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite( AttributeQueryRunnerTest.class);
    return suite;
  }

  /**
   * Test a result set not requiring batching, without a hardlimit
   * @throws Exception
   */
  public void testSmallResultSetNoLimit() throws Exception {
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));

    executeQuery(q, 0);
  }

  /**
   * Test a big result set requiring batching, without a hardlimit
   * @throws Exception
   */
  public void testBigResultSetNoHardLimit() throws Exception {
    int hardLimit = NO_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a big result set requiring batching, with a small hardlimit ( < result set)
   * @throws Exception
   */
  public void testBigResultSetSmallHardLimit() throws Exception {
    int hardLimit = SMALL_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a big result set requiring batching, with a big hardlimit (> result set)
   * @throws Exception
   */
  public void testBigResultSetBigHardLimit() throws Exception {
    int hardLimit = BIG_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a small list not requiring list batching, with a small result not requiring
   * batching, no hardlimit
   * @throws Exception
   */
  public void testSmallListSmallResultSetNoHardLimit() throws Exception {
    int hardLimit = NO_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL_NUM));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
        
    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }

  /**
   * Test a small list not requiring batching, with a small resultset not requiring 
   * batching, with a small hardlimit (< result set)
   * @throws Exception
   */
  public void testSmallListSmallResultSetSmallHardLimit() throws Exception {
    int hardLimit = SMALL_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL_NUM));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
    
    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a small list not requiring batching, with a small resultset not requiring
   * batching, with a big hardlimit (> resultset)
   * @throws Exception
   */
  public void testSmallListSmallResultSetBigHardLimit() throws Exception {
    int hardLimit = BIG_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL_NUM));
    q.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
    
    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a small list not requiring batching, with a big resultset requiring
   * batching, no hardlimit
   * @throws Exception
   */
  public void testSmallListBigResultSetNoHardLimit() throws Exception {
    int hardLimit = NO_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));

    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));
    
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }

  /**
   * Test a small list not requiring batching, with a big resultset requiring
   * batching, and a small hardlimit (< resultset)
   * @throws Exception
   */
  public void testSmallListBigResultSetSmallHardLimit() throws Exception {
    int hardLimit = SMALL_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));

    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a small list not requiring batching, with a big resultset requiring
   * batching, and a big hardlimit (> resultset)
   * @throws Exception
   */
  public void testSmallListBigResultSetBigHardLimit() throws Exception {
    int hardLimit = BIG_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));

    //create a small resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }
  
  /**
   * Test a big list requiring batching, with a small result set not requiring
   * batching, and no hardlimit
   * @throws Exception
   */
  public void testBigListSmallResultSetNoHardLimit() throws Exception {
    int hardLimit = NO_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL_NUM));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "22"));
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }

  /**
   * Test a big list requiring batching, with a small result set not requiring
   * batching, and a small hardlimit (< result set)
   * @throws Exception
   */
  public void testBigListSmallResultSetSmallHardLimit() throws Exception {
    int hardLimit = SMALL_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL_NUM));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "1"));
    
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }

  /**
   * Test a big list requiring batching, with a small result set not requiring
   * batching, and a big hardlimit (> result set)
   * @throws Exception
   */
  public void testBigListSmallResultSetBigHardLimit() throws Exception {
    int hardLimit = BIG_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    q.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "1"));

    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));

    executeQuery(q, hardLimit);
  }

  /**
   * Test a big list requiring batching, with a big result set requiring
   * batching, and a no hardlimit
   * @throws Exception
   */
  public void testBigListBigResultSetNoHardLimit() throws Exception {
    int hardLimit = NO_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BasicFilter("chr_name","main","gene_id_key", "=", "18"));

    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));
    
    executeQuery(q, hardLimit);
  }

  /**
   * Test a big list requiring batching, with a big result set requiring
   * batching, and a small hardlimit (< resultset)
   * @throws Exception
   */
  public void testBigListBigResultSetSmallHardLimit() throws Exception {
    int hardLimit = SMALL_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    q.addFilter(new BooleanFilter("transmembrane_bool", "main", "gene_id_key", BooleanFilter.isNULL));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL));
    
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));
    
    executeQuery(q, hardLimit);
  }

  /**
   * Test a big list requiring batching, with a big result set requiring
   * batching, and a big hardlimit (> resultset)
   * @throws Exception
   */  
  public void testBigListBigResultSetBigHardLimit() throws Exception {
    int hardLimit = BIG_HARD_LIMIT;
    Query q = new Query(genequery);
    q.addAttribute(new FieldAttribute("hgbaseid", "hsapiens_gene_ensembl__snp__dm", "transcript_id_key"));
    q.addFilter(new BooleanFilter("transmembrane_bool", "main", "gene_id_key", BooleanFilter.isNULL));
    
    //create a big resultset subquery and add it to main q
    Query subq = new Query(genequery);
    subq.addAttribute(new FieldAttribute("gene_stable_id","main","gene_id_key"));
    subq.addFilter(new BooleanFilter("disease_gene_bool","main","gene_id_key", BooleanFilter.isNotNULL));
    
    q.addFilter(new IDListFilter("gene_stable_id","main","gene_id_key", subq));
    
    executeQuery(q, hardLimit);
  }
  
  private void executeQuery(Query q, int hardLimit) throws Exception {
    StatOutputStream stats = new StatOutputStream();
    engine.execute(q, new FormatSpec(FormatSpec.TABULATED), stats, hardLimit);

    int linesReturned = stats.getLineCount();
    assertTrue("No text returned from query", stats.getCharCount() > 0);
    if (hardLimit > 0)
      assertTrue(linesReturned + " Lines Returned greater than hardLimit "+ hardLimit +"\n", hardLimit >= linesReturned);
    else
      assertTrue("No lines returned from query", linesReturned > 0);

    stats.close();
  }
}
