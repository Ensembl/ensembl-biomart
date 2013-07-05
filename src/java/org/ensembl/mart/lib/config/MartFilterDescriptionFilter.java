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

import org.jdom.Element;

/**
 * JDOM Filter specific to FilterDescription/MapFilterDescription objects in a Mart FilterCollection.
 * Allows MartConfigurationFactory to harvest all FilterDescription objects of both types, in the order
 * they occur in the xml configuration document.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class MartFilterDescriptionFilter extends BaseMartElementFilter {
  
  public MartFilterDescriptionFilter(boolean includeHiddenMembers) {
    super(includeHiddenMembers);
  }
  
	/* (non-Javadoc)
	 * @see org.jdom.filter.Filter#matches(java.lang.Object)
	 */
	public boolean matches(Object obj) {
    boolean ret = super.matches(obj);
		
    if (ret) {
			Element e = (Element) obj;
      ret = (e.getName().equals(UIFILTER) || e.getName().equals(UIDSFILTER));
		}
		return ret;
	}

  private final String UIFILTER = "FilterDescription";
  private final String UIDSFILTER = "MapFilterDescription";
}
