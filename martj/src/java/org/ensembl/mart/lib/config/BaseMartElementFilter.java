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


import java.util.logging.Logger;

import org.jdom.Element;
import org.jdom.filter.Filter;

/** 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public abstract class BaseMartElementFilter implements Filter {

  private Logger logger = Logger.getLogger(BaseMartElementFilter.class.getName());
  private final String HIDDEN = "hidden";
  private boolean includeHiddenMembers = true;

  public BaseMartElementFilter(boolean includeHiddenMembers) {
    this.includeHiddenMembers = includeHiddenMembers;
  }

  /* (non-Javadoc)
   * @see org.jdom.filter.Filter#matches(java.lang.Object)
   */
  public boolean matches(Object obj) {
    boolean ret = (obj instanceof Element);

    if (ret && !includeHiddenMembers) {        
        Element e = (Element) obj;

        //if hidden = true, ret should be false
        boolean hidden = Boolean.valueOf(e.getAttributeValue(HIDDEN)).booleanValue();
        ret = !hidden;
        
//        //now skip placeholder attributes and filters
//        if (ret && ( (e.getName().equals("FilterDescription")) 
//         || (e.getName().equals("AttributeDescription")) ) ) {
//           ret = !(e.getAttributeValue("internalName").matches("\\w+\\.\\w+"));
//        }
    }
    
    return ret;
  }

}
