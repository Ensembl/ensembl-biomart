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
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.lib;

import javax.sql.DataSource;

import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * The listener interface for receiving query events.
 * 
 * A class that is interested in processing a query event
 * will implement this interface. The listener object created from 
 * that class is then registered with a
 * Query using the query's <code>addQueryListener(listener)</code> 
 * method. When the query's status changes by virtue of having one of 
 * it's properties changed the relevant method in the listener object is 
 * invoked.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
 * @see Query
 */
public interface QueryListener {

  void queryNameChanged(Query sourceQuery, String oldName, String newName);

  void datasetChanged(
    Query source,
    String oldDataset,
    String newDataset);

  void datasourceChanged(
    Query sourceQuery,
    DataSource oldDatasource,
    DataSource newDatasource);

  void attributeAdded(Query sourceQuery, int index, Attribute attribute);

  void attributeRemoved(Query sourceQuery, int index, Attribute attribute);

  void filterAdded(Query sourceQuery, int index, Filter filter);

  void filterRemoved(Query sourceQuery, int index, Filter filter);

  void filterChanged(
    Query sourceQuery,
    int index,
    Filter oldFilter,
    Filter newFilter);

  void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription newSequenceDescription);

  void limitChanged(Query query, int oldLimit, int newLimit);

  void starBasesChanged(
    Query sourceQuery,
    String[] oldStarBases,
    String[] newStarBases);

  void primaryKeysChanged(
    Query sourceQuery,
    String[] oldPrimaryKeys,
    String[] newPrimaryKeys);

  void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig);

}
