package org.ensembl.mart.lib;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
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
 * UnprocessedFilterHandler implementing object designed to process
 * URL type IDListFilter objects into STRING type IDListFilter objects.
 * Currently only supports file: URLs with one or more ids, one per line.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class URLIDListFilterHandler extends IDListFilterHandlerBase {

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, java.util.List, org.ensembl.mart.lib.Query)
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query) throws InvalidQueryException {
		Query newQuery = new Query(query);
		for (int i = 0, n = filters.size(); i < n; i++) {
			IDListFilter idfilter = (IDListFilter) filters.get(i);
			newQuery.removeFilter(idfilter);
			
			URL idURL = idfilter.getUrl();
			String[] unversionedIds = null;
		
			if (idURL.getProtocol().equals("file")) {
				try {
					unversionedIds = HarvestStream( new InputStreamReader( idURL.openStream() ) );
				} catch (IOException e) {
					throw new InvalidQueryException( "Problem reading from file", e );
				}
			}
			else 
			//impliment HTML parser here
			throw new InvalidQueryException("Non File URLs are not currently supported\n"); 
		
			if (unversionedIds.length > 0)
				newQuery.addFilter(new IDListFilter(idfilter.getField(), idfilter.getTableConstraint(), idfilter.getKey(), unversionedIds));
		}
		return newQuery;
	}

}
