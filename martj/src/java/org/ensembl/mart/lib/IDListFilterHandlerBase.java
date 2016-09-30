package org.ensembl.mart.lib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 * Base UnprocessedFilterHandler implementing object that provides a private method
 * to handle versioned ids in a manner appropriate to the dataset.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public abstract class IDListFilterHandlerBase implements UnprocessedFilterHandler {

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.UnprocessedFilterHandler#ModifyQuery(org.ensembl.mart.lib.Engine, java.util.List, org.ensembl.mart.lib.Query)
	 */
	public abstract Query ModifyQuery(Engine engine, List filters, Query query) throws InvalidQueryException;

	/**
	 * Harvests an InputStreamReader for IDS, one per line into a String[]
	 * 
	 * @param instream - InputStreamReader object with IDs, one per line.
	 * @return String[] list of IDs harvested from instream
	 * @throws InvalidQueryException for all underlying exceptions
	 */
	protected String[] HarvestStream(InputStreamReader instream)
		throws InvalidQueryException {
		String[] harvestedIds;
		try {
			List identifiers = new ArrayList();
			BufferedReader in = new BufferedReader(instream);

			for (String line = in.readLine(); line != null; line = in.readLine())
				identifiers.add(line);

			harvestedIds = new String[identifiers.size()];
			identifiers.toArray(harvestedIds);
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Problem getting IDs from Stream: " + e.getMessage());
			throw new InvalidQueryException("Could not harvest IDs from Stream: " + e.getMessage(), e);
		}

		if (harvestedIds.length < 1) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("No IDS harvested from Stream\n");
		}
    
    return harvestedIds;
	}

	protected Logger logger = Logger.getLogger(IDListFilterHandlerBase.class.getName());
}
