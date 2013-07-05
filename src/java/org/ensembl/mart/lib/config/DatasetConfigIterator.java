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
package org.ensembl.mart.lib.config;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

/** 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public class DatasetConfigIterator implements Iterator {

  private Iterator current = null;
  private Vector nextIterators = new Vector();
  private Iterator nextIter = nextIterators.iterator();
  
  public DatasetConfigIterator(Iterator datasetviews) {
    current = datasetviews;
  }
  
  public void addDatasetConfigIterator(DatasetConfigIterator iter) {
    nextIterators.add(iter);
    nextIter = nextIterators.iterator(); //refresh the iterator
  }
  
  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  public boolean hasNext() {
    boolean ret = current.hasNext();
    
    if (!ret) {
      if (nextIter.hasNext()) {
        current = (Iterator) nextIter.next();
        ret = true;
      }
    }
    
    return ret;
  }

  //TODO: this method currently returns a non lazyLoaded DatasetConfig object, and defers loading to the lazyLoad system. When MartExplorer and MartEditor are redesigned to better manage DatasetConfig objects in memory, this should me made to return preLoaded DatasetConfig objects
  
  /* (non-Javadoc)
   * @see java.util.Iterator#next()
   */
  public Object next(){
    if (!hasNext())
      throw new NoSuchElementException("There Are no more DatasetConfigs in this DatasetConfigIterator\n");

    DatasetConfig next = null;
    try {
      next = new DatasetConfig((DatasetConfig) current.next(), false, false);
    } catch (ConfigurationException e) {
      //ignore, because we are not violating the contract for the exception
    } //lazyloaded copy
    return (Object) next;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
