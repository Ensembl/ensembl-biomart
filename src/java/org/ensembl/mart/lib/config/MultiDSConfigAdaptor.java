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
 * Further defines common functionality for DSConfigAdaptor implimentations
 * which contain Multiple DatasetConfig objects within them.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public interface MultiDSConfigAdaptor extends DSConfigAdaptor {

  /**
   * Remove a DatasetConfig from the adaptor.  This might mean removing one dataset config
   * from a MultiDSConfigAdaptor, or removing an entire adaptor.
   * @param dsv -- DatasetConfig to remove
   * Returns boolean, true if removed, false if not removed
   * @throws ConfigurationException for an underlying Exceptions
   */
  public boolean removeDatasetConfig(DatasetConfig dsv) throws ConfigurationException;

}
