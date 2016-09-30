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
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.DatasetConfig;


/**
 * This UnprocessedFilterHandler implementing object resolves Requests for
 * Genes/Snps located between known chromosomal markers into
 * a chromosomal start coordinate BasicFilter, or a chromosomal 
 * end coordinate BasicFilter.
 * 
 * @author <a href="mailto:damian@ebi.ac.uk">Damian Smedley</a>
 */
public class GenericHandler implements UnprocessedFilterHandler {

	private Logger logger = Logger.getLogger(GenericHandler.class.getName());

	/* (non-Javadoc)
	 * @see org.ensembl.mart.explorer.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, java.util.List, org.ensembl.mart.lib.Query)
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query)
		throws InvalidQueryException {
		Connection conn = null;
		try {
			conn = query.getDataSource().getConnection();

			Query newQuery = new Query(query);

			String sql, lookUpTable, filterName, filterQualifier, filterValue;
			PreparedStatement ps;
			Filter chrFilter;

			for (int i = 0, n = filters.size(); i < n; i++) {
				Filter element = (Filter) filters.get(i);
				newQuery.removeFilter(element);

				String field = element.getField();
				String value = element.getValue();
				
				boolean start = false;
				boolean end = false;
				
				if (field.endsWith("_start"))
				  start = true;
				else if (field.endsWith("_end"))
				  end = true;  
				
				lookUpTable = null;
				//String marker_value = element.getValue();
				//if (element.getHandler().equals("org.ensembl.mart.lib.GenericHandler")) {
				//	lookUpTable = element.getTableConstraint();
				//} else
				//	throw new InvalidQueryException(
				//		"Recieved invalid handler for GenericHandler " + field + "\n");

				
				DatabaseMetaData dmd = conn.getMetaData();

				List filt_cols = new ArrayList();
				List look_cols = new ArrayList();
				
				ResultSet rset = dmd.getColumns(null, null, lookUpTable, null);
				while (rset.next()) {
				  if (rset.getString(3).toLowerCase().equals(lookUpTable.toLowerCase())) {
					String cname = rset.getString(4);
					if (cname.startsWith("filt_")){
					  if (start){
					  	if (cname.endsWith("_start"))	
					      filt_cols.add(cname.replaceFirst("filt_",""));  	
					  }
					  else if (end){
						if (cname.endsWith("_end"))	
				          filt_cols.add(cname.replaceFirst("filt_","")); 
					  }
					  else
					    filt_cols.add(cname.replaceFirst("filt_","")); 
					}
					else if (cname.startsWith("olook_"))// && !(cname.equals(field)))
					  look_cols.add(cname.replaceFirst("olook_",""));  	
				  }
				}
				rset.close();
			    
				String[] newfilterCols = new String[filt_cols.size()];
				filt_cols.toArray(newfilterCols);

				String[] lookCols = new String[look_cols.size()];
				look_cols.toArray(lookCols);
				
				StringBuffer buf = new StringBuffer("SELECT ");
				
				for (int k = 0; k < newfilterCols.length; ++k) {
				  if (k > 0)
					buf.append(" , ");
				  buf.append("filt_" + newfilterCols[k]);
				}			    
                
                buf.append(" FROM ");
                buf.append(lookUpTable);
                buf.append(" WHERE " + field + "='" + value + "'");
                
                boolean and = false;
                
				for (int k = 0; k < lookCols.length; k++) {
				  
				  Filter filt = query.getFilterByName(lookCols[k]);
				  
				  if (filt == null)
				     filt = newQuery.getFilterByName(lookCols[k]);
				  if (filt == null){//may be in the same lookup table
				     filt = query.getFilterByName("olook_" + lookCols[k]);  
					 newQuery.removeFilter(filt);
				  } 
				  if (filt == null)
					throw new InvalidQueryException("Requires a particular Filter to have already been added to the Query." + lookCols[k]);
                  buf.append(" AND ");
                  buf.append("olook_" + lookCols[k] + "='" + filt.getValue() + "'");
				}			    
			    
				sql = buf.toString();
			    
				logger.info(
									"SQL: "
										+ sql
										+ "\nparameter 1: "
										//+ chrvalue
										+ " parameter 2: "
										//+ marker_value
										+ "\n");
       
				ps = conn.prepareStatement(sql);
				
				ResultSet rs = ps.executeQuery();
				rs.next();
				if (rs.isLast()){//only 1 row returned
					
					//rs.next(); // will only be one result
				    for (int k = 0; k < newfilterCols.length; ++k) {
				      filterValue = rs.getString(k+1);
				  
				      logger.info("Recieved filterValue " + filterValue + " from SQL\n");
				  
				      if (filterValue != null && filterValue.length() > 0) {
					    DatasetConfig dsv = query.getDatasetConfig();
					    FilterDescription fd = dsv.getFilterDescriptionByInternalName(newfilterCols[k]);
					    Filter posFilter =
					  	  new BasicFilter(fd.getField(), fd.getTableConstraint(), fd.getKey(), fd.getQualifier(), filterValue);
					    newQuery.addFilter(posFilter);
				      } else
					    throw new InvalidQueryException("Did not recieve a filterValue ");
				    }				  
				}
				else{//multiple rows - need to set an ID list filter
					
					List tranIds = new ArrayList();
					tranIds.add(rs.getString(1));
					while(rs.next())
						tranIds.add(rs.getString(1));
					
					String[] tids = new String[tranIds.size()];
					tranIds.toArray(tids);
					//set the id list filter(s) - code for just 1 per lookup table at moment - may need changing in future
				
					if (tranIds.size() > 0) {
						DatasetConfig dsv = query.getDatasetConfig();
						FilterDescription fd = dsv.getFilterDescriptionByInternalName(newfilterCols[0]);
						
						Filter posFilter =
							//new IDListFilter(fd.getFieldFromContext(), fd.getTableConstraintFromContext(), fd.getKeyFromContext(), tids);
						new IDListFilter(fd.getField(newfilterCols[0]), fd.getTableConstraint(newfilterCols[0]), fd.getKey(newfilterCols[0]), tids);
						
						newQuery.addFilter(posFilter);
					 } else
						  throw new InvalidQueryException("Did not recieve a filterValue ");
				}
		    }

			return newQuery;
		} catch (SQLException e) {
			throw new InvalidQueryException(
				"Recieved SQLException " + e.getMessage());
		} finally {
			DetailedDataSource.close(conn);
		}

	}
}
