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

import java.util.ArrayList;
import java.util.List;

/**
 * Object encapsulating a MartRegistry.dtd compliant xml document.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class MartRegistry implements Comparable {
  //needs to maintain the order of locations without a virtualSchema, and virtualSchema
  //defined locations
  private List elements = new ArrayList();
  
  public void addVirtualSchema(virtualSchema vSchema) {
      elements.add(vSchema);
  }
  
  public void addMartLocation(MartLocation dsvl) {
    elements.add(dsvl);
  }
  
  public Object[] getElementsInOrder() {
      return (Object[]) elements.toArray(new Object[elements.size()]);
  }
  
  public String toString() {
      StringBuffer buf = new StringBuffer();
      
      buf.append("[");
      
      for (int i = 0, n = elements.size(); i < n; i++) {
          if ( i>0 ) buf.append(", ");
          buf.append( elements.get(i).toString() );
      }
      
      buf.append("]");
      
      return buf.toString();
  }
  
  /**
   * Based on the hashCode.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Object o) {
    return hashCode() - o.hashCode();
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    int hashcode = 0;
    
    for (int i = 0, n = elements.size(); i < n; i++) {
      Object loc = elements.get(i);
      hashcode += loc.hashCode();
    }
    
    return hashcode;
  }

  /**
   * Allows Equality Comparisons manipulation of MartRegistry objects
   */
  public boolean equals(Object o) {
    return o instanceof MartRegistry && hashCode() == o.hashCode();
  }
}
