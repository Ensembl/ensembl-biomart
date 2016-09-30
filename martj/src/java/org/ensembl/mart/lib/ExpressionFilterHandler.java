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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * UnprocessedFilterHandler implementing object designed to process requests for Genes fitting
 * defined expression profiles defined by expression datasets provided by the Mart.
 * All Expression related Filters supplied in the List are resolved into a single IDListFilter of transcript ids 
 * added to the query.
 * The Filter field must match the following format:
 * expression_dataset:Term
 * examples:
 *   est:anatomical_site
 *   gnf:anatomical_site
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class ExpressionFilterHandler implements UnprocessedFilterHandler {

	private final String VALIDSQL =
		"select count(*) from global__evoc_vocabulary__look where term = ?";

	private Logger logger =
		Logger.getLogger(ExpressionFilterHandler.class.getName());

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, java.util.List, org.ensembl.mart.lib.Query)
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query)
		throws InvalidQueryException {

		Connection conn = null;

		try {
			conn = query.getDataSource().getConnection();

			Query newQuery = new Query(query);
			//must get species and dataset, from the first starBase
			String species = null;
			String dataset = null;
			String focus = null;
			String dset = null;

			//resolve dataset, species, and focus
			String[] mainTables = newQuery.getMainTables();

			for (int i = 0; i < mainTables.length; i++) {
				if (mainTables[i].matches(".*gene__main"))
					dataset = mainTables[i];
			}

			if (dataset == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning(
						"Could not determine dataset for query, perhaps it is a snp query "
							+ newQuery);
				throw new InvalidQueryException("Could not determine dataset for query, perhaps it is a snp query ");
			}

			StringTokenizer tokens = new StringTokenizer(dataset, "_", false);
			species = tokens.nextToken();
			//focus = tokens.nextToken();
			//dset = species + "_" + focus;
            dset = dataset.split("__")[0];
			String trans_lib_table = null;
			StringBuffer idSQL = new StringBuffer();
			// set on first_table, append lidBuf later
			StringBuffer lidBuf = new StringBuffer(" where lib_id in (");

			int terms = 0;
			String firstTable = null;

			//	will build up SQL
			StringBuffer selectBuf = new StringBuffer("select ");
			StringBuffer fromBuf = new StringBuffer(" from ");
			StringBuffer whereBuf = new StringBuffer(" where ");

			List values = new ArrayList();
			String edataset = null;

			for (int i = 0, n = filters.size(); i < n; i++) {
				Filter element = (Filter) filters.get(i);
				newQuery.removeFilter(element);

				String field_info = element.getField();
				if (!(field_info.indexOf(".") > 0))
					throw new InvalidQueryException(
						"Recieved invalid field for Expression Filter, should be of the form x:y where x is the expression dataset name, and y is the term "
							+ field_info
							+ "\n");

				String value = element.getValue();
				StringTokenizer ptokens = new StringTokenizer(field_info, ".");

				if (edataset == null)
					edataset = ptokens.nextToken();
				else {
					String tmp = ptokens.nextToken();
					if (!(edataset.equals(tmp)))
						throw new InvalidQueryException(
							"Sorry, mixing expression datasets not valid, recieved both "
								+ edataset
								+ " and "
								+ tmp
								+ "\n");
				}

				String term = ptokens.nextToken();
				values.add(value);

				if (!IsValidTerm(conn, value))
					throw new InvalidQueryException(
						"Term " + term + " does not exist in the Mart Database\n");

				String table =
					species + "__expression_" + edataset + "_" + term + "__sup";
				if (terms < 1) {
					firstTable = table;
					selectBuf.append(firstTable + ".lib_id");
					trans_lib_table = dset + "__expression_" + edataset + "__look";
					idSQL.append("select transcript_stable_id from " + trans_lib_table);
					// will only get the lib_id for the first term, but mapped across all support tables in the from and where clause
				}

				if (terms > 0) {
					fromBuf.append(" , ");
					whereBuf.append(
						" AND " + firstTable + ".lib_id = " + table + ".lib_id AND ");
				}
				fromBuf.append(table);
				whereBuf.append(table + ".term = ?");

				terms++;
			}

			String sql = selectBuf.append(fromBuf).append(whereBuf).toString();

            if (logger.isLoggable(Level.INFO))
			  logger.info("Getting lib_ids with " + sql);

			PreparedStatement lps = conn.prepareStatement(sql);

			for (int i = 0, n = values.size(); i < n; i++) {
				String element = (String) values.get(i);
        
                if (logger.isLoggable(Level.INFO))
                  logger.info("bind " + i + " = " + element + "\n");

				lps.setString(i + 1, element);
			}
			ResultSet lrs = lps.executeQuery();

			int lidcount = 0;
			while (lrs.next()) {
				if (lidcount > 0)
					lidBuf.append(", ");

				lidBuf.append("\"").append(lrs.getString(1)).append("\"");
				lidcount++;
			}
			lps.close();
			lrs.close();
			lidBuf.append(")");


            if (lidcount == 0){
				throw new InvalidQueryException("No libraries mapped to this term\n");
            }
			idSQL.append(lidBuf);
			sql = idSQL.toString();

			logger.info("Getting ids with " + sql);

			List tranIds = new ArrayList();

			PreparedStatement tps = conn.prepareStatement(sql);
			ResultSet trs = tps.executeQuery();

			while (trs.next())
				tranIds.add(trs.getString(1));

			tps.close();
			trs.close();

			String[] tids = new String[tranIds.size()];
			tranIds.toArray(tids);

			newQuery.addFilter(new IDListFilter("transcript_stable_id", "main", "transcript_id_key", tids));
			return newQuery;
		} catch (SQLException e) {
			throw new InvalidQueryException(
				"Recieved SQL Exception processing request for Expression Filter "
					+ e.getMessage(),
				e);
		} finally {
      DetailedDataSource.close( conn );
		}
	}

	private boolean IsValidTerm(Connection conn, String term)
		throws SQLException {
		boolean valid = true;
		PreparedStatement ps = conn.prepareStatement(VALIDSQL);
		ps.setString(1, term);

		ResultSet rs = ps.executeQuery();
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		ps.close();

		if (!(count > 0))
			valid = false;

		return valid;
	}
}
