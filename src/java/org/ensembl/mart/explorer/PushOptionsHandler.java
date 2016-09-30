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

import java.util.logging.Logger;

import org.ensembl.mart.lib.config.PushAction;

/**
 * Active wrapper for a PushOption instance. It is able to fetch the 
 * named target from the group and push / remove options from it. 
 * Targets are retrieved when needed because they might not exist when 
 * the component is created.
 */
public class PushOptionsHandler {

  private Logger logger = Logger.getLogger(PushOptionsHandler.class.getName());

	private FilterGroupWidget group;

	private PushAction optionPush;
  
  
  private FilterWidget getTargetFilterWidget() {
    return group.getFilterWidget( optionPush.getRef() );
  }

	/**
	 * @param optionPush Option push object containing the name of the target filter
   * and the options to be pushed onto it or removed from it.
	 * @param group filter group from which the target filter can be retrieved.
	 */
	public PushOptionsHandler(PushAction optionPush, FilterGroupWidget group) {
		this.optionPush = optionPush;
    this.group = group;
	}

  /**
   * Set the specified options on the target filter.
   */
  public void push(){
    FilterWidget w = getTargetFilterWidget(); 
    w.setOptions( optionPush.getOptions() );
  }
  
  /**
   * Remove all options from the target filter.
   */
  public void remove() {
    getTargetFilterWidget().setOptions( null );
  }

}
