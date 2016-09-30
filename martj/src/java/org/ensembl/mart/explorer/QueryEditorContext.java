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


/**
 * The interface for a class that manages query editors.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public interface QueryEditorContext {

  /**
   * @return all queryEditors.
   */
  QueryEditor[] getQueryEditors();
  
  /**
   * Remove the specified editor if present.
   * @param queryEditor editor to be removed, does nothing if
   * editor is null or not present.
   */
  void remove(QueryEditor editor);

}
