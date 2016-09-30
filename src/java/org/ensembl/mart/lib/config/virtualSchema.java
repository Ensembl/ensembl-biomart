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
 * Object encapsulating a virtualSchema element within a
 * MartRegistry.dtd compliant xml document.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class virtualSchema implements Comparable {
  private List martLocations = new ArrayList();
  private String name;
  
  public virtualSchema(String name) {
      this.name = name;
  }
  
  public String getName() {
      return name;
  }
  
  public void addMartLocation(MartLocation dsvl) {
    martLocations.add(dsvl);
  }
  
  public MartLocation[] getMartLocations() {
    return (MartLocation[]) martLocations.toArray(new MartLocation[martLocations.size()]);
  }
  
  public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[").append("name=").append(name).append(" locations [");
    
        for (int i = 0, n = martLocations.size(); i < n; i++) {
          if ( i>0 ) buf.append(", ");
            buf.append( martLocations.get(i).toString() );
        }
    		
        buf.append("]").append("]");

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
    
    //do not need to track order of locations
    for (int i = 0, n = martLocations.size(); i < n; i++) {
      MartLocation loc = (MartLocation) martLocations.get(i);
      hashcode += loc.hashCode();
    }
    
    return hashcode;
  }

  /**
   * Allows Equality Comparisons manipulation of MartRegistry objects
   */
  public boolean equals(Object o) {
    return o instanceof virtualSchema && hashCode() == o.hashCode();
  }
}
