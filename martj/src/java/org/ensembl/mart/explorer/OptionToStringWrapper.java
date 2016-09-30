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

package org.ensembl.mart.explorer;

import org.ensembl.mart.lib.config.Option;


/**
 * Simple wapper for an option with a toString() that returns the 
 * option's displayName. Useful for wrapping an option for insertion 
 * in a JComboList.
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
 */
class OptionToStringWrapper {
  
  public final FilterWidget OptionToStringProxy;
  public final Option option;

  public OptionToStringWrapper(FilterWidget widget, Option o) {
    this.option = o;
    this.OptionToStringProxy = widget;
  }

  /**
   * @return option.getDisplayName()
   */
  public String toString() {
    return option.getDisplayName();
  }
  
}