/*
 Copyright (C) 2006 EBI
 
 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the itmplied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.biomart.builder.view.gui.diagrams.contexts;

import java.util.Collection;

import javax.swing.JComponent;
import javax.swing.JPopupMenu;

import org.biomart.builder.view.gui.diagrams.Diagram;

/**
 * The diagram context receives notification to populate context menus in
 * {@link Diagram}s, or to change the colours of objects displayed in the
 * diagram. All objects in the diagram are passed to both methods at some point,
 * so anything displayed can be customised.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.12 $, $Date: 2007-08-08 11:50:55 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public interface DiagramContext {
	/**
	 * Customise the appearance of a component that represents the given
	 * database object.
	 * 
	 * @param component
	 *            the component that represents the object.
	 * @param object
	 *            the database object we wish to customise this component to.
	 */
	public void customiseAppearance(JComponent component, Object object);

	/**
	 * Add items to a context menu for a given database object. Should add a
	 * separator first if the menu is not empty.
	 * 
	 * @param contextMenu
	 *            the context menu to add parameters to.
	 * @param object
	 *            the database object we wish to customise this menu to.
	 */
	public void populateContextMenu(JPopupMenu contextMenu, Object object);

	/**
	 * Add items to a context menu for a collection of database objects. Should
	 * add a separator first if the menu is not empty.
	 * 
	 * @param contextMenu
	 *            the context menu to add parameters to.
	 * @param selectedItems
	 *            the database objects we wish to customise this menu to.
	 * @param clazz
	 *            the type of objects in the selection.
	 */
	public void populateMultiContextMenu(JPopupMenu contextMenu,
			Collection selectedItems, Class clazz);

	/**
	 * Tests to see if the specified object is masked. This is used in the
	 * show/hide masked objects switch.
	 * 
	 * @param object
	 *            the object to test.
	 * @return <tt>true</tt> if it is masked.
	 */
	public boolean isMasked(Object object);
}
