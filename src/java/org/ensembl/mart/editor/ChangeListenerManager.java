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

package org.ensembl.mart.editor;

import java.util.ArrayList;
import java.util.List;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * Manages a list of ChangeListeners and provides a method for
 * sending events to them all.
 **/
public class ChangeListenerManager {
	
	private List changeListeners = new ArrayList();

	public void addChangeListener(ChangeListener listener) {
		changeListeners.add( listener );
	}

	public boolean removeChangeListener(ChangeListener listener) {
			return changeListeners.remove( listener );
	}

	public void stateChanged( ChangeEvent event ) {
		for (int i = 0, n = changeListeners.size(); i < n; i++) {
			ChangeListener listener = (ChangeListener) changeListeners.get(i);
			listener.stateChanged( event );
		}
	}
}
