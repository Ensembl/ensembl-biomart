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

import java.util.List;

/**
 * Interface for Handlers for Filter objects that require processing into a new Filter, or set of Filters 
 * without a handler requirement.  The system takes an existing Engine (may not be used by all 
 * implimentations of this Interface, but note that Engine.getConnection() returns a Database Connection as well), 
 * List of Filter Objects for a specified Handler implimentation, and a Query as its argument to ModifyQuery.  
 * It then creates a copy of the query, removing each handled Filter, and then processes the Filters into one or 
 * more Filters that are added to the new query.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public interface UnprocessedFilterHandler {
	
	/**
	 * Method to modify the Query by resolving a given Filter object into
	 * a Filter not requiring a handler.  It will have an engine to execute Mart Query
	 * Objects, or extract the database connection, if necessary.  
	 * The Method should use the Query copy constructor to make a new Copy of the Query, 
	 * modify it, and return the new Query.
	 * 
	 * @param engine - Engine object
	 * @param idfilter - Filter object having a handler
	 * @param query - Query object to be modified.
	 * @return Query Object with one or more non-handler Filters
	 * @throws InvalidQueryException -- chains all underlying Exceptions as InvalidQueryExceptions
	 */
	public Query ModifyQuery(Engine engine, List filters, Query query) throws InvalidQueryException;

}
