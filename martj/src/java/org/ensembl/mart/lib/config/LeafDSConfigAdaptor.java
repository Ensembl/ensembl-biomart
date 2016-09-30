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

/**
 * DSConfigAdaptor implementations that do not contain child DSConfigAdaptor objects
 * can extend this object to implement getAdaptorXXX methods that return null or empty objects. 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public abstract class LeafDSConfigAdaptor {

  /**
   * LeafDSConfigAdaptor objects do not contain child adaptors
   * @return null
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorByName(java.lang.String)
   */
  public DSConfigAdaptor getAdaptorByName(String adaptorName) throws ConfigurationException {
    // DatabaseDSConfigAdaptor objects do not contain child adaptors
    return null;
  }

  /**
   * LeafDSConfigAdaptor Objects do not contain child DSConfigAdaptor Objects.
   * @return empty String[]
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorNames()
   */
  public String[] getAdaptorNames() throws ConfigurationException {
    return new String[0];
  }

  /**
    * LeafDSConfigAdaptor Objects do not contain child DSConfigAdaptor Objects.
    * @return empty DSConfigAdaptor[] 
    * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getLeafAdaptors()
    */
   public DSConfigAdaptor[] getLeafAdaptors() throws ConfigurationException {
     return new DSConfigAdaptor[0];
   }

  /**
   * LeafDSConfigAdaptor objects do not contain child adaptors
   * return false
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsAdaptor(java.lang.String)
   */
  public boolean supportsAdaptor(String adaptorName) throws ConfigurationException {
    return false;
  }
}
