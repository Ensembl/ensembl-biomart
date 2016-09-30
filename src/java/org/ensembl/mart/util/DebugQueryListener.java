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

package org.ensembl.mart.util;

import java.io.PrintStream;

import javax.sql.DataSource;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.QueryListener;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Simple class that listens to a query and prints it's state to the
 * specified stream everytime the query changes. Useful for debugging.
 *
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
 */
public class DebugQueryListener implements QueryListener {

  private PrintStream os;

  /**
   * @param os output stream to pring debug messages.
   */
  public DebugQueryListener(PrintStream os) {
    this.os = os;
  }

  public void queryNameChanged(
    Query sourceQuery,
    String oldName,
    String newName) {
    os.println(sourceQuery);
  }

  public void datasetChanged(
    Query source,
    String oldDataset,
    String newDataset) {
    os.println(source);
  }

  public void datasourceChanged(
    Query sourceQuery,
    DataSource oldDatasource,
    DataSource newDatasource) {
    os.println(sourceQuery);
  }

  public void attributeAdded(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    os.println(sourceQuery);
  }

  public void attributeRemoved(
    Query sourceQuery,
    int index,
    Attribute attribute) {
    os.println(sourceQuery);
  }

  public void filterAdded(Query sourceQuery, int index, Filter filter) {
    os.println(sourceQuery);
  }

  public void filterRemoved(Query sourceQuery, int index, Filter filter) {
    os.println(sourceQuery);
  }

  public void filterChanged(
    Query sourceQuery,
    int index,
    Filter oldFilter,
    Filter newFilter) {
    os.println(sourceQuery);
  }

  public void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription newSequenceDescription) {
    os.println(sourceQuery);
  }

  public void limitChanged(Query query, int oldLimit, int newLimit) {
    os.println(query);
  }

  public void starBasesChanged(
    Query sourceQuery,
    String[] oldStarBases,
    String[] newStarBases) {
    os.println(sourceQuery);
  }

  public void primaryKeysChanged(
    Query sourceQuery,
    String[] oldPrimaryKeys,
    String[] newPrimaryKeys) {
    os.println(sourceQuery);
  }

  public void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig) {
    os.println(query);
  }

}
