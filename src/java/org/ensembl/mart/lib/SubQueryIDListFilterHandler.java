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

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * UnprocessedFilterHandler implementing object designed to process SUBQUERY
 * type IDListFilter objects into STRING type IDListFilter objects.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class SubQueryIDListFilterHandler extends IDListFilterHandlerBase {

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, java.util.List, org.ensembl.mart.lib.Query)
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query) throws InvalidQueryException {
		Query newQuery = new Query(query);
		query = null;
    
		for (int i = 0, n = filters.size(); i < n; i++) {
			IDListFilter idfilter = (IDListFilter) filters.get(i);
			newQuery.removeFilter(idfilter);
			
			Query subq = idfilter.getSubQuery();
      
			ByteArrayOutputStream idstream = new ByteArrayOutputStream();
      String[] ids = null;
    
			try {
				engine.execute(subq, FormatSpec.TABSEPARATEDFORMAT, idstream, 0, true);

				ids = idstream.toString().split("\n+");
				idstream.close();
			} catch (Exception e) {
				throw new InvalidQueryException("Could not execute subquery: "+ e.getMessage());
			}
		
			if (ids.length > 0)
				newQuery.addFilter(new IDListFilter(idfilter.getField(), idfilter.getTableConstraint(), idfilter.getKey(),ids));
		}
						
		return newQuery;
	}

}
