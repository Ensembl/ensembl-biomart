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

import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class for interaction between UI and Mart Database.  Manages mySQL database
 * connections, and executes Querys.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */

//TODO: implement broad(transcript based) versus narrow(gene based) filtering of resultsets

public class Engine {
  private static Logger logger = Logger.getLogger(Engine.class.getName());

  /**
   * Attempts to load the database drivers normally shipped with 
   * martlib distribution.
   */
  private static void loadFallbackDatabaseDrivers() {
    String[] driverNames = new String[] { "org.gjt.mm.mysql.Driver" };
    for (int i = 0; i < driverNames.length; i++) {
      try {
        Class.forName(driverNames[i]).newInstance();
      } catch (Exception e) {
        if (logger.isLoggable(Level.WARNING))
          logger.warning("Failed to load driver" + driverNames[i]);
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  /** 
   * Load drivers normally distributed with mart lib. These will be
   * available if no other drivers are previously loaded.
   */
  {
    loadFallbackDatabaseDrivers();
  }

  public Engine() {
  }

  public void countFocus(OutputStream os, Query oquery) throws InvalidQueryException, SQLException {
    PrintStream pstream = new PrintStream(os, true); //autoflush true
    //ensure that we are using a copy of the Query
    Query query = new Query(oquery);

    //TODO: this may be removed
    //remove any attributes present, so that they do not affect the count
    if (query.getAttributes().length > 0)
      query.removeAllAttributes();

    //process any unprocessed filters
    Hashtable needsHandler = new Hashtable();

    Filter[] filters = query.getFilters();
    for (int i = 0, n = filters.length; i < n; i++) {
      Filter filter = filters[i];
      if (filter instanceof IDListFilter) {
        IDListFilter idfilter = (IDListFilter) filter;
        if (idfilter.getHandler() != null) {
          String handler = idfilter.getHandler();
          if (!needsHandler.containsKey(handler))
            needsHandler.put(handler, new ArrayList());

          List unhandledFilters = (ArrayList) needsHandler.get(handler);
          if (!unhandledFilters.contains(filter))
            unhandledFilters.add(filter);
          needsHandler.put(handler, unhandledFilters);
        } 
      }
    }

    for (Iterator iter = needsHandler.keySet().iterator(); iter.hasNext();) {
      String handler = (String) iter.next();
      List unprocessedFilters = (ArrayList) needsHandler.get(handler);
      UnprocessedFilterHandler idhandler = UnprocessedFilterHandlerFactory.getInstance(handler);
      query = idhandler.ModifyQuery(this, unprocessedFilters, query);
    }
    
    DetailedDataSource dsource = query.getDataSource();
    QueryCompiler csql = new QueryCompiler(query,dsource);
    String fcountSQL = csql.toFocusCountSQL();
    writeSQLResults(pstream, query, fcountSQL);
  }

  private void writeSQLResults(PrintStream pstream, Query query, String sql) throws InvalidQueryException {
    DetailedDataSource dsource = query.getDataSource();
    if (dsource == null)
      throw new InvalidQueryException("Query must have a DataSource to execute against\n");

    Connection conn = null;
    try {
      conn = dsource.getConnection();
      PreparedStatement ps = conn.prepareStatement(sql);

      int p = 1;
      for (int i = 0, n = query.getFilters().length; i < n; ++i) {
        Filter f = query.getFilters()[i];
        String value = f.getValue();
        if (value != null) {
          logger.fine("SQL (prepared statement value) : " + p + " = " + value);
          ps.setString(p++, value);
        }
      }

      ResultSet rs = ps.executeQuery();

      if (rs.next())
        pstream.print(rs.getString(1) + "\n");
      else
        pstream.print("0\n");
    } catch (SQLException e) {
      if (logger.isLoggable(Level.WARNING))
        logger.warning(e.getMessage());
      throw new InvalidQueryException(e);
    } finally {
      DetailedDataSource.close(conn);
    }

  }

  /**
   * Checks for DomainSpecificFilters in the Query, and uses the DSFilterHandler
   * system to modify the Query accordingly, if present.
   * Constructs a QueryRunner object for the given Query, and format using 
   * a QueryRunnerFactory.  Uses the QueryRunner to execute the Query
   * with the mySQL connection of this Engine, and write the results to 
   * a specified OutputStream.
   * 
   * @param query - A Query Object
   * @param formatspec - A FormatSpec Object
   * @param os - An OutputStream
   * @throws FormatException - unsupported Format supplied to the QueryRunnerFactory
   * @throws SequenceException - general Exception thrown for a variety of reasons that the SeqQueryRunners cannot write out sequence data
   * @throws InvalidQueryException - general Exception thrown when invalid query parameters have been presented, and the resulting SQL will not work.
   * @see Query
   * @see FormatSpec
   * @see QueryRunnerFactory
   * @see QueryRunner
   * @see UnprocessedFilterHandler
   * @see UnprocessedFilterHandlerFactory
   */
  public void execute(Query query, FormatSpec formatspec, OutputStream os)
    throws SequenceException, FormatException, InvalidQueryException, SQLException {

    if (query.hasLimit())
      execute(query, formatspec, os, query.getLimit());
    else
      execute(query, formatspec, os, 0);

  }

  /**
   * Checks for DomainSpecificFilters in the Query, and uses the DSFilterHandler
   * system to modify the Query accordingly, if present.
   * Constructs a QueryRunner object for the given Query, and format using 
   * a QueryRunnerFactory.  Applies a limit clause to the SQL.
   * Uses the QueryRunner to execute the Query with the mySQL connection of 
   * this Engine, and write the results to a specified OutputStream.
   * 
   * @param query A Query Object
   * @param formatspec A FormatSpec Object
   * @param os An OutputStream
   * @param limit limits the number of records returned by the query
   * @throws SequenceException
   * @throws FormatException
   * @throws InvalidQueryException
   * @see Query
   * @see FormatSpec
   * @see QueryRunnerFactory
   * @see QueryRunner
   * @see UnprocessedFilterHandler
   * @see UnprocessedFilterHandlerFactory
   */
  public void execute(Query query, FormatSpec formatspec, OutputStream os, int limit)
    throws SequenceException, FormatException, InvalidQueryException, SQLException {

    execute(query, formatspec, os, limit, false);
  }

  /**
   * allows the client to add a limit to the number of rows returned for a query.
   * @param query
   * @param formatspec
   * @param os
   * @param limit
   * @param isSubQuery
   * @throws SequenceException
   * @throws FormatException
   * @throws InvalidQueryException
   * @throws SQLException
   * @see Query
   * @see FormatSpec
   * @see QueryRunnerFactory
   * @see QueryRunner
   * @see UnprocessedFilterHandler
   * @see UnprocessedFilterHandlerFactory
   */
  public void execute(Query query, FormatSpec formatspec, OutputStream os, int limit, boolean isSubQuery)
    throws SequenceException, FormatException, InvalidQueryException, SQLException {

    //must initialize query for sequence queries specially
    if (query.getType() == Query.SEQUENCE) {
      //make a copy to prevent changes being filtered back to the client
      query = new Query(query);
      query.initializeForSequence();
    }
    
    //process any unprocessed filters
    Hashtable needsHandler = new Hashtable();

    Filter[] filters = query.getFilters();
    
    for (int i = 0, n = filters.length; i < n; i++) {
      Filter filter = filters[i];
      if (filter instanceof IDListFilter) {
        IDListFilter idfilter = (IDListFilter) filter;
        if (idfilter.getHandler() != null) {
          String handler = idfilter.getHandler();
          if (!needsHandler.containsKey(handler))
            needsHandler.put(handler, new ArrayList());
          
          List unhandledFilters = (ArrayList) needsHandler.get(handler);
          if (!unhandledFilters.contains(filter))
            unhandledFilters.add(filter);
          needsHandler.put(handler, unhandledFilters);
        } 
      }
    }

    for (Iterator iter = needsHandler.keySet().iterator(); iter.hasNext();) {
      String handler = (String) iter.next();
      List unprocessedFilters = (ArrayList) needsHandler.get(handler);
      UnprocessedFilterHandler idhandler = UnprocessedFilterHandlerFactory.getInstance(handler);
      query = idhandler.ModifyQuery(this, unprocessedFilters, query);
    }

    logger.fine(query.toString());
    
    QueryRunner qr = QueryRunnerFactory.getInstance(query, formatspec, os);
    qr.execute(limit, isSubQuery);
  }

  public String sql(Query query) {
    throw new RuntimeException();
  }

}
