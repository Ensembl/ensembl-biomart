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

package org.ensembl.mart.lib;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.util.FormattedSequencePrintStream;

/** 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public abstract class BaseSeqQueryRunner implements QueryRunner {

  protected int maxColumnLen = 80;

  //batching numbers
  protected final int[] batchModifiers = { 5, 2 };
  protected int modIter = 0; //start at 0 
  protected int batchLength = 1000;
  private final int maxBatchLimit = 750000;
  
  // total number of rows execute will ever return
//  private final int MAXTOTALROWS = 999999999;

  //big list batching
  private final int listSizeMax = 1000;
  private final int maxBigListCount = 1;
    
  protected String separator;
  private Logger logger = Logger.getLogger(BaseSeqQueryRunner.class.getName());

  protected Query query = null;
  protected Attribute[] attributes = null;
  protected Filter[] filters = null;
  protected FormatSpec format = null;
  protected FormattedSequencePrintStream osr;
  protected SeqWriter seqWriter;

  protected int totalRows = 0;
  protected int totalRowsThisExecute = 0;
  protected int resultSetRowsProcessed = 0; // will count rows processed for a given ResultSet batch
  protected int lastID = -1;
  protected int lastIDRowsProcessed = 0;
  // will allow process to skip rows already processed in previous batch, for a given ID

  protected TreeMap iDs = new TreeMap();
  // holds each objects information, in order
  protected List fields = new ArrayList();
  // holds unique list of resultset description fields from the query

  // Used for colating required fields
  protected String queryID, qualifiedQueryID;
  protected String coordStart, coordEnd;
  protected String rankField;
  protected String chrField;
  protected String strandField;

  // need to know these indexes specifically
  protected int queryIDindex = -1;
  protected int rankIndex = -1;
  protected int startIndex = -1;
  protected int endIndex = -1;
  protected int chromIndex = -1;
  protected int strandIndex = -1;
  protected List otherIndices = new ArrayList();
  protected DNAAdaptor dna;

  public BaseSeqQueryRunner(Query query) {
    this.query = new Query(query);
  }

  /**
   * This method should set the required variables queryID, coordStart, and coordEnd to
   * the values necessary for the type of sequence being processed, then 
   * add any other displayID fields required for output, etc. to the displayIDs List, 
   * and finally update the Query object with attributes that are necessary to get 
   * the sequence data.
   */
  protected abstract void updateQuery();

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryRunner#execute(int)
   */
  public void execute(int limit) throws SequenceException, InvalidQueryException {
    execute(limit, false);
  }

  protected ResultSet skipNewBatchRedundantRecords(ResultSet rs) throws SQLException {
    if (lastID > -1) {
      //If lastID > -1, we know that there are 1 or more rows to skip before beginning to process again
      while ((resultSetRowsProcessed < lastIDRowsProcessed) && rs.next()) {
        //skip through rows already processed for a the last id, incrementing only resultSetRowsprocessed
        //This will only occur at the beginning of a new ResultSet batch

        resultSetRowsProcessed++;
      }
    }

    return rs;
  }

  /**
   * This Method should first calculate the indices of the various fields that it requires,
   * using the ResultSet Object ResultSetMetaData.  It should then iterate through each of the
   * results in the ResultSet, processing them.  While processing, if it should encounter a new 
   * keyID, it should write out the results from the lastID, and reset lastIDRowsProcessed to zero.
   * It should end this loop by incrementing the totalRows, resultSetRowsProcessed and lastIDRowsProcessed
   * integers, and setting the lastID to the current keyID.intValue.
   *  
   * @param conn
   * @param rs
   * @throws IOException
   * @throws SQLException
   */
  protected abstract void processResultSet(Connection conn, ResultSet rs) throws IOException, SQLException;
  
  protected void writeLastEntry(Connection conn) throws SequenceException {
    // write the last transcripts data, if present
    if (lastID > -1)
      seqWriter.writeSequences(new Integer(lastID), conn);
  }

  protected abstract class SeqWriter {
    abstract void writeSequences(Integer tranID, Connection conn) throws SequenceException;
  }
  
  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryRunner#execute(int, boolean)
   */
  public void execute(int hardLimit, boolean isSubQuery) throws SequenceException, InvalidQueryException {
    if (isSubQuery)
      throw new SequenceException("SubQuerys cannot return sequences\n");

    //Some implementations do not need DNA adaptors, so they will have null SeqInfo
    if (query.getSequenceDescription().getSeqInfo() != null 
     && query.getSequenceDescription().getSeqInfo().length() > 0)
      dna = new DNAAdaptor(query.getSequenceDescription());
    
    updateQuery();
        
    Filter[] filters = query.getFilters();

    Filter bigListFilter = null;
    String[] biglist = null;
    int numBigLists = 0;
    for (int i = 0, n = filters.length; i < n; i++) {
      Filter filter = filters[i];
      if (filter instanceof IDListFilter) {
        if (((IDListFilter) filter).getIdentifiers().length > listSizeMax) {
          if (numBigLists > maxBigListCount)
            throw new InvalidQueryException("Too many in list filters attached, only one per query supported.\n");

          bigListFilter = filter;
          biglist = ((IDListFilter) filter).getIdentifiers();
          numBigLists++;
        }
      }
    }

    if (numBigLists > 0) {      
      boolean moreRows = true;
      String[] idBatch = new String[listSizeMax];
      int batchIter = 0;

      for (int i = 0, n = biglist.length; moreRows && i < n; i++) {
        String element = biglist[i];

        if ((i > 0) && ((i % listSizeMax) == 0)) {
          Query newQuery = new Query(query);
          newQuery.removeFilter(bigListFilter);

          IDListFilter newFilter =
            new IDListFilter(bigListFilter.getField(), bigListFilter.getTableConstraint(), bigListFilter.getKey(), idBatch);
          newQuery.addFilter(newFilter);

          executeQuery(newQuery, hardLimit);

          if (hardLimit > 0)
            moreRows = totalRows < hardLimit;
              
          idBatch = new String[listSizeMax];
          batchIter = 0;
        }
        idBatch[batchIter] = element;
        batchIter++;
      }

      //last batch is either empty, or less than idBatch.length
      if (moreRows && idBatch[0] != null) {
        
        List lastBatch = new ArrayList();
        for (int i = 0, n = idBatch.length; i < n; i++) {
          String element = idBatch[i];
          if (element != null)
            lastBatch.add(element);
        }

        String[] lbatch = new String[lastBatch.size()];
        lastBatch.toArray(lbatch);
                  
        Query newQuery = new Query(query);
        newQuery.removeFilter(bigListFilter);

        IDListFilter newFilter = new IDListFilter(bigListFilter.getField(), bigListFilter.getTableConstraint(), bigListFilter.getKey(),lbatch);
        newQuery.addFilter(newFilter);

        executeQuery(newQuery, hardLimit);
      }
    } else {
      executeQuery(query, hardLimit);
    }
    
    if (dna != null)
      dna.close();
  }

  protected void executeQuery(Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    //System.out.println("HARD LIMIT IS\t" + hardLimit);

    DetailedDataSource ds = curQuery.getDataSource();
    if (ds == null)
      throw new RuntimeException("curQuery.DataSource is null");

    if (ds.getDatabaseType().equals("mysql")) {
      //mySQL solution
      executeQueryMysql(ds, curQuery, hardLimit);
    } else {
      //generic solution
      executeQueryGeneric(ds, curQuery, hardLimit);
    }
  }
  
  protected void executeQueryMysql(DetailedDataSource ds, Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    boolean moreRows = true;
    boolean userLimit = false;
    totalRowsThisExecute = 0;

    attributes = curQuery.getAttributes();
    filters = curQuery.getFilters();

    Connection conn = null;
    String sql = null;
    try {
      conn = ds.getConnection();

      QueryCompiler csql = new QueryCompiler(curQuery,ds);
      String sqlbase = csql.toSQL();

      while (moreRows) {
        sql = sqlbase;

        sql += " order by "
            + qualifiedQueryID;
            
        int maxRows = 0;
        if (hardLimit > 0) {
          userLimit = true;
          maxRows = Math.min(batchLength, hardLimit - totalRows);
          moreRows = false;
        } else
          maxRows = batchLength;

        sql += " LIMIT " + totalRowsThisExecute + "," + maxRows;
        
        if (logger.isLoggable(Level.INFO))
          logger.info("SQL : " + sql + "\n");

        PreparedStatement ps = conn.prepareStatement(sql);

        int p = 1;
        for (int i = 0; i < filters.length; ++i) {
          Filter f = filters[i];
          String value = f.getValue();

          if (value != null) {
            logger.info("SQL (prepared statement value) : " + p + " = " + value);
            ps.setString(p++, value);
          }
        }

        ResultSet rs = ps.executeQuery();
        resultSetRowsProcessed = 0;

        processResultSet(conn, rs);

        // on the odd chance that the last result set is equal in size to the batchLength, it will need to make an extra attempt.
        if ((!userLimit) && (resultSetRowsProcessed < batchLength))
          moreRows = false;

        if (batchLength < maxBatchLimit) {
          batchLength *= batchModifiers[modIter];
          modIter = (modIter == 0) ? 1 : 0;
        }

        rs.close();
      }

      writeLastEntry(conn);
      conn.close();
    } catch (IOException e) {
      throw new SequenceException(e);
    } catch (SQLException e) {
      throw new InvalidQueryException(e + " :" + sql);
    } finally {
      DetailedDataSource.close(conn);
    }
  }
  
  protected void executeQueryGeneric(DetailedDataSource ds, Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    boolean moreRows = true;
    boolean userLimit = false;

    attributes = curQuery.getAttributes();
    filters = curQuery.getFilters();

    Connection conn = null;
    String sql = null;
    try {
      conn = curQuery.getDataSource().getConnection();

      QueryCompiler csql = new QueryCompiler(curQuery,ds);
      String sqlbase = csql.toSQL();

      while (moreRows) {
        sql = sqlbase;

        if (lastID > -1) {
          if (sqlbase.indexOf("WHERE") >= 0)
            sql += " and " + qualifiedQueryID + " >= " + lastID;
          else
            sql += " WHERE " + qualifiedQueryID + " >= " + lastID;
        }

        sql += " order by " + qualifiedQueryID;

        if (logger.isLoggable(Level.INFO)) {
          logger.info("SQL : " + sql + "\n");
          logger.info("batchLength : " + batchLength + "\n");
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        if (hardLimit > 0) {
          userLimit = true;
          ps.setMaxRows(hardLimit);
          moreRows = false;
        } else
          ps.setMaxRows(batchLength);

        int p = 1;
        for (int i = 0; i < filters.length; ++i) {
          Filter f = filters[i];
          String value = f.getValue();

          if (value != null) {
            logger.info("SQL (prepared statement value) : " + p + " = " + value);
            ps.setString(p++, value);
          }
        }

        ResultSet rs = ps.executeQuery();
        resultSetRowsProcessed = 0;

        processResultSet(conn, skipNewBatchRedundantRecords(rs));

        // on the odd chance that the last result set is equal in size to the batchLength, it will need to make an extra attempt.
        if ((!userLimit) && (resultSetRowsProcessed < batchLength))
          moreRows = false;

        if (batchLength < maxBatchLimit) {
          batchLength *= batchModifiers[modIter];
          modIter = (modIter == 0) ? 1 : 0;
        }

        rs.close();
      }

      writeLastEntry(conn);
      conn.close();
    } catch (IOException e) {
      throw new SequenceException(e);
    } catch (SQLException e) {
      throw new InvalidQueryException(e + " :" + sql);
    } finally {
      DetailedDataSource.close(conn);
    }
  }
}
