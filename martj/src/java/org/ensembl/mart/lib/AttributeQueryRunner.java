package org.ensembl.mart.lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implimentation of the QueryRunner for executing a Query and 
 * generating Tabulated output.
 * Tabulated output is separated by a field separator specified by 
 * a FormatSpec object.  Any Query can generate tabulated output.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see Query
 * @see FormatSpec
 */
public final class AttributeQueryRunner implements QueryRunner {

  /**
   * Constructs a TabulatedQueryRunner object to execute a Query
   * and print tabulated output specified by the given FormatSpec
   * 
   * @param query - a Query Object
   * @param format - a FormatSpec object
   */
  public AttributeQueryRunner(Query query, FormatSpec format, OutputStream os) {
    this.query = query;
    this.format = format;
    this.osr = new PrintStream(os, true); // autoflush true
  }

  public void execute(int hardLimit) throws SequenceException, InvalidQueryException {
    execute(hardLimit, false);
  }

  public void execute(int hardLimit, boolean isSubQuery) throws SequenceException, InvalidQueryException {
    if (hardLimit > 0)
      hardLimit = Math.min(hardLimit, MAXTOTALROWS);
    else if (!isSubQuery)
      hardLimit = MAXTOTALROWS;

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
            new IDListFilter(
              bigListFilter.getField(),
              bigListFilter.getTableConstraint(),
              bigListFilter.getKey(),
              idBatch);
          newQuery.addFilter(newFilter);

          executeQuery(newQuery, hardLimit);

          if (isSubQuery) {
            //          get all ids for a subQuery
            moreRows = true;
          } else {
            //only execute batches until all are completed, or totalRows == hardLimit
            moreRows = totalRows < hardLimit;
          }

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

        IDListFilter newFilter =
          new IDListFilter(bigListFilter.getField(), bigListFilter.getTableConstraint(), bigListFilter.getKey(), lbatch);
        newQuery.addFilter(newFilter);

        executeQuery(newQuery, hardLimit);
      }
    } else {
      executeQuery(query, hardLimit);
    }
  }

  protected void executeQuery(Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    //System.out.println("HARD LIMIT IS\t" + hardLimit);

    DetailedDataSource ds = curQuery.getDataSource();
    if (ds == null)
      throw new RuntimeException("curQuery.DataSource is null");

    if (ds.getDatabaseType().equals("mysql") || ds.getDatabaseType().equals("postgres")) {
      executeQueryPostgresMysql(ds, curQuery, hardLimit);
    } else {
      executeQueryGeneric(ds, curQuery, hardLimit);
    }
  }
  
  protected void executeQueryGeneric(DetailedDataSource ds, Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    lastID = -1; // so nothing is skipped
    attributes = curQuery.getAttributes();
    filters = curQuery.getFilters();
    boolean moreRows = true;

    Connection conn = null;
    String sql = null;
    
    
    try {
      csql = new QueryCompiler(curQuery,ds);
      String sqlbase = csql.toSQLWithKey();
      String primaryKey = csql.getQualifiedLowestLevelKey();
      //queryID = csql.getPrimaryKey();
      queryID = csql.getLowestLevelKey();

      conn = ds.getConnection();

      while (moreRows) {
        StringBuffer sqlBuf = new StringBuffer(sqlbase);

        if (sqlbase.indexOf("WHERE") >= 0) {
          String insert = primaryKey + " >= " + lastID + " AND ";
          sqlBuf.insert(sqlbase.indexOf("WHERE") + 6, insert);
        } else
          sqlBuf.append(" WHERE " + primaryKey + " >= " + lastID);

        if (!( curQuery.hasSort() ) )
          sqlBuf.append(" ORDER BY " + primaryKey);

        sql = sqlBuf.toString();
        
        int maxRows = 0;
        if (hardLimit > 0)
          maxRows = Math.min(batchLimit, hardLimit - totalRows);
        else
          maxRows = batchLimit;

        if (logger.isLoggable(Level.INFO)) {
            logger.info("SQL : " + sql);
          }
        
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setMaxRows(maxRows);
         
        //System.out.println("MAX ROWS\t" + maxRows); 
        int p = 1;
        for (int i = 0, n = filters.length; i < n; ++i) {
          Filter f = curQuery.getFilters()[i];
          String value = f.getValue();
          if (value != null) {
            logger.fine("SQL (prepared statement value) : " + p + " = " + value);
            ps.setString(p++, value);
          }
        }

        ResultSet rs = ps.executeQuery();
        resultSetRowsProcessed = 0;
        processResultSetGeneric(conn, skipNewBatchRedundantRecords(rs));

        // on the odd chance that the last result set is equal in size to the batchLength, it will need to make an extra attempt.
        if (resultSetRowsProcessed < batchLimit) {
          // this is the last batch - hence set moreRows to false
          moreRows = false;
        }
        if (batchLimit < maxBatchLimit) {
          batchLimit =
            (batchLimit * batchModifiers[modIter] < maxBatchLimit)
              ? batchLimit * batchModifiers[modIter]
              : maxBatchLimit;
          modIter = (modIter == 0) ? 1 : 0;
        }
        //else          
        //batchLimit += linearIncrease;
        ps.close();
        rs.close();
      }
    } catch (IOException e) {
      if (logger.isLoggable(Level.WARNING))
        logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
      throw new InvalidQueryException(e);
    } catch (SQLException e) {
      if (logger.isLoggable(Level.WARNING))
        logger.warning(e.getMessage());
      throw new InvalidQueryException(e);
    } finally {
      DetailedDataSource.close(conn);
    }
  }
  
  protected void executeQueryPostgresMysql(DetailedDataSource ds, Query curQuery, int hardLimit) throws SequenceException, InvalidQueryException {
    attributes = curQuery.getAttributes();
    filters = curQuery.getFilters();
    boolean moreRows = true;
    totalRowsThisExecute = 0;

    Connection conn = null;
    String sql = null;
    
    
    try {
      csql = new QueryCompiler(curQuery,ds);
      String sqlbase = csql.toSQL();

      conn = ds.getConnection();

      while (moreRows) {
        sql = sqlbase;

        int maxRows = 0;
        if (hardLimit > 0)
          maxRows = Math.min(batchLimit, hardLimit - totalRows);
        else
          maxRows = batchLimit;

        if (ds.getDatabaseType().equals("mysql")) {sql += " LIMIT " + totalRowsThisExecute + "," + maxRows;} //;(maxRows - lastIDRowsProcessed); 
        if (ds.getDatabaseType().equals("postgres")) {
        	//int pslimit=maxRows-totalRowsThisExecute;
        	sql += " LIMIT " + maxRows + " OFFSET " + totalRowsThisExecute;}
        

        if (logger.isLoggable(Level.INFO))
          logger.info("SQL : " + sql);

        PreparedStatement ps = conn.prepareStatement(sql);

        int p = 1;
        for (int i = 0, n = filters.length; i < n; ++i) {
          Filter f = curQuery.getFilters()[i];
          String value = f.getValue();
          if (value != null) {
            logger.fine("SQL (prepared statement value) : " + p + " = " + value);
            ps.setString(p++, value);
          }

  }

        ResultSet rs = ps.executeQuery();

        resultSetRowsProcessed = 0;
        processResultSetMysql(conn, rs);

        // on the odd chance that the last result set is equal in size to the batchLength, it will need to make an extra attempt.
        if (resultSetRowsProcessed < batchLimit) {
          // this is the last batch - hence set moreRows to false
          moreRows = false;
        }
        if (batchLimit < maxBatchLimit) {
          batchLimit =
            (batchLimit * batchModifiers[modIter] < maxBatchLimit) ? batchLimit * batchModifiers[modIter] : maxBatchLimit;
          modIter = (modIter == 0) ? 1 : 0;
        }
        //else          
        //batchLimit += linearIncrease;

        rs.close();
      }
    } catch (IOException e) {
      if (logger.isLoggable(Level.WARNING))
        logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
      throw new InvalidQueryException(e);
    } catch (SQLException e) {
      if (logger.isLoggable(Level.WARNING))
        logger.warning(e.getMessage());
      throw new InvalidQueryException(e);
    } finally {
      DetailedDataSource.close(conn);
    }
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
    //System.out.println(resultSetRowsProcessed + "\t" + lastIDRowsProcessed);

    return rs;
  }

  private void processResultSetMysql(Connection conn, ResultSet rs) throws IOException, SQLException {
    while (rs.next()) {
      for (int i = 1, nColumns = rs.getMetaData().getColumnCount(); i <= nColumns; ++i) {
        if (i > 1)
          osr.print(format.getSeparator());
        String v = rs.getString(i);

        if (v != null)
          osr.print(v);
      }
      osr.print("\n");

      if (osr.checkError())
        throw new IOException();

      totalRows++;
      totalRowsThisExecute++;
      resultSetRowsProcessed++;
    }
  }

  private void processResultSetGeneric(Connection conn, ResultSet rs) throws IOException, SQLException {
    ResultSetMetaData rmeta = rs.getMetaData();

    int queryIDindex = 0;

    // process columnNames for required attribute indices
    for (int i = 1, nColumns = rmeta.getColumnCount(); i <= nColumns; ++i) {
      String column = rmeta.getColumnName(i);

      if (column.toLowerCase().equals(queryID.toLowerCase()))
        queryIDindex = i;
    }

    if ((lastIDRowsProcessed == maxBatchLimit) && rs.isLast()) {
      throw new SQLException("WARNING - MORE THAN 50000 ROWS FOR A SINGLE ID BREAKS THE CURRENT BATCHING SYSTEM");
    }

    while (rs.next()) {
      int currID = rs.getInt(queryIDindex);
      //System.out.println("CURR ID" + currID + "\tlastID\t" + lastID);
      if (lastID > -1 && lastID != currID) {
        lastIDRowsProcessed = 0;

        //reset batchLimit to maxBatchLength if it has needed to creep up to finish the last ids results
        if (batchLimit > maxBatchLimit)
          batchLimit = maxBatchLimit;
      }

      for (int i = 1, nColumns = rs.getMetaData().getColumnCount(); i <= nColumns; ++i) {
        //skip the queryID
        if (i != queryIDindex) {
          if (i > 1)
            osr.print(format.getSeparator());
          String v = rs.getString(i);

          if (v != null)
            osr.print(v);
          //          else
          //            osr.print("NULL");
        }
      }
      osr.print("\n");

      if (osr.checkError())
        throw new IOException();

      lastID = currID;
      totalRows++;
      resultSetRowsProcessed++;
      lastIDRowsProcessed++;
    }
  }

  //batching 
  private final int[] batchModifiers = { 2, 2 };
  private int modIter = 0; //start at 0 
  private int batchLimit = 5000; //5000;
  private final int maxBatchLimit = 50000; //50000;

  // total number of rows execute will ever return
  private final int MAXTOTALROWS = 100000000; //50000;

  //allow batchLength to increase by this amount after maxBatchLength has been reached
  //this will result in slow response for queries where each id returns a resultset
  //larger than maxBatchLimit, but they will eventually finish
  //and the system will reset the limit back to the batch for each new id
  //this could, concievably, hit a memory limit, so test and tweak
  //  private final int linearIncrease = 10; 

  //big list batching
  private final int listSizeMax = 1000;
  private final int maxBigListCount = 1;

  private String queryID = null;
  private int lastID = -1;
  private int totalRows = 0;
  private int totalRowsThisExecute = 0;
  
  private int resultSetRowsProcessed = 0; // will count rows processed for a given ResultSet batch
  private int lastIDRowsProcessed = 0;
  // will allow process to skip rows already processed in previous batch, for a given ID

  private Logger logger = Logger.getLogger(AttributeQueryRunner.class.getName());
  private Query query = null;
  private QueryCompiler csql;
  private Attribute[] attributes = null;
  private Filter[] filters = null;
  private FormatSpec format = null;
  private PrintStream osr;
}
