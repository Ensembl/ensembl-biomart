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
 * Interface to all Objects that point to Mart DatasetConfig document collections. 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public interface MartLocation {

  /**
   * Allows client to determine the type of a particular MartLocation implimenting
   * object, before casting to actually use it.  This must return one of the MartLocationBase static
   * enums.
   * @return String , one of the MartLocationBase static String enums 
   */
  public String getType();
  
  /**
   * Returns the name of the Location.  Each implimentation must return a suitable default value, instead of null.
   * @return String name of the location
   */
  public String getName();
  
  /**
   * set the name of the Location
   */
  public void setName(String name);
  
  /**
   * Determine if this is a visible Mart
   * @return boolean
   */
  public boolean isVisible();
  
  /**
   * Set whether this Mart is visible
   * @param visible boolean
   */
  public void setVisible(boolean visible);
}
