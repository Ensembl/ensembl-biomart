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
 * An abstract adapter class for receiving query events.
 * The methods in this class are empty. This class exists as
 * convenience for creating listener objects.
 * <P>
 * Extend this class to create a <code>Query</code> listener 
 * and override the methods for the events of interest.
 * </P>
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * 
 * @see org.ensembl.mart.lib.QueryListener
 */
public abstract class QueryAdaptor implements QueryListener {

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#queryNameChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void queryNameChanged(
    Query sourceQuery,
    String oldName,
    String newName) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#datasetChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void datasetChanged(
    Query source,
    String oldDataset,
    String newDataset) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#datasourceChanged(org.ensembl.mart.lib.Query, javax.sql.DataSource, javax.sql.DataSource)
   */
  public void datasourceChanged(
    Query sourceQuery,
    DataSource oldDatasource,
    DataSource newDatasource) {
    

  }

  /*
   * @see org.ensembl.mart.lib.QueryListener#attributeAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
   */
  public void attributeAdded(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#attributeRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
   */
  public void attributeRemoved(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#filterAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterAdded(Query sourceQuery, int index, Filter filter) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#filterRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterRemoved(Query sourceQuery, int index, Filter filter) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#filterChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter, org.ensembl.mart.lib.Filter)
   */
  public void filterChanged(
    Query sourceQuery,
    int index,
    Filter oldFilter,
    Filter newFilter) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#sequenceDescriptionChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.SequenceDescription, org.ensembl.mart.lib.SequenceDescription)
   */
  public void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription newSequenceDescription) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#limitChanged(org.ensembl.mart.lib.Query, int, int)
   */
  public void limitChanged(Query query, int oldLimit, int newLimit) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#starBasesChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void starBasesChanged(
    Query sourceQuery,
    String[] oldStarBases,
    String[] newStarBases) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#primaryKeysChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void primaryKeysChanged(
    Query sourceQuery,
    String[] oldPrimaryKeys,
    String[] newPrimaryKeys) {
    

  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryListener#datasetConfigChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.config.DatasetConfig, org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig) {
    

  }

}
