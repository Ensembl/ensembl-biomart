package org.ensembl.mart.lib;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.List;

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

/**
 * UnprocessedFilterHandler implementing object designed to process File type
 * IDListFilter objects into STRING type IDListFilter objects. Expects that 
 * files contain one or more ids, one per line.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class FileIDListFilterHandler extends IDListFilterHandlerBase {

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, org.ensembl.mart.lib.IDListFilter, org.ensembl.mart.lib.Query)
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query) throws InvalidQueryException {
		Query newQuery = new Query(query);

		for (int i = 0, n = filters.size(); i < n; i++) {
			IDListFilter idfilter = (IDListFilter) filters.get(i);
			newQuery.removeFilter(idfilter);

			File idFile = idfilter.getFile();
			String[] unversionedIds = null;
      
      Connection conn = null;
			try {
        conn = query.getDataSource().getConnection();
				unversionedIds =
					HarvestStream( new InputStreamReader(new FileInputStream(idFile)));
			} catch (Exception e) {
				throw new InvalidQueryException("Could not parse File IDListFilter: " + e.getMessage(), e);
			} finally {
        DetailedDataSource.close( conn );
			}

			if (unversionedIds.length > 0)
				newQuery.addFilter(new IDListFilter(idfilter.getField(), idfilter.getTableConstraint(), idfilter.getKey(), unversionedIds));
		}

		return newQuery;
	}

}
