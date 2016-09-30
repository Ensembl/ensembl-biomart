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

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.prefs.Preferences;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.ComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * Compound widget that contains a JComboBox and optional radio button. 
 * Additional behaviour includes broadcasting change events to listeners
 * when state changes and support for converting list of items to/from
 * strings (useful for storing in history files).
 *
 */
public class LabelledComboBox extends Box implements ActionListener {
	
	private final int GAP = 5;
	
	private ChangeListenerManager changeListenerManager;
	private ChangeEvent changeEvent = null;
	private JRadioButton radioButton = null;
	private JComboBox combo = null;
	private String preferenceKey = null;
	
	public LabelledComboBox(String label) {
		this( label, null, null);	
	}
	
	public LabelledComboBox(String label, ChangeListener listener) {
		this(label, listener, null);
	}

	public LabelledComboBox(String label, ChangeListener listener, ButtonGroup group) {
		super( BoxLayout.X_AXIS);
		
		changeListenerManager = new ChangeListenerManager();
		
		if ( group!=null ) {
			radioButton = new JRadioButton( label );
			radioButton.addActionListener( this );
			add( radioButton );
		} else {
			add( new JLabel( label ) );
		}
		
		if ( listener!=null ) {
			changeListenerManager.addChangeListener( listener );
		}
		
		changeEvent = new ChangeEvent(this);
		
		add( Box.createHorizontalStrut( GAP*2 ) );
		combo = new JComboBox();
		combo.setEditable( true );
		combo.addActionListener( this );
		add( combo );
		
		Dimension dim = new Dimension(500, 35);
		setPreferredSize( dim );
		setMaximumSize( dim );
		setBorder( new EmptyBorder(GAP, GAP, GAP, GAP) );
	}



	/**
	 * Called when the user alters the contents of the Component. Sets the radio button to selected
	 * (if it is available) and broadcasts change messages to all change listeners. 
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	public void actionPerformed(ActionEvent e) {
          
		// this enables us to select the radio button if the combobox value changes
		if ( radioButton!=null ) {
			radioButton.setSelected( true );
		}
		
		changeListenerManager.stateChanged( changeEvent );
	}
	
	
	
	
	/**
	 * @param listener
	 */
	public void addChangeListener(ChangeListener listener) {
		changeListenerManager.addChangeListener(listener);
	}

	/**
	 * @param listener
	 * @return
	 */
	public boolean removeChangeListener(ChangeListener listener) {
		return changeListenerManager.removeChangeListener(listener);
	}

	
	/**
	 * Currently selected text.
	 * @return currently selected text, or "" if none selected.
	 */
	public String getText() {
		String text="";
		if ( combo.getSelectedItem()!=null ) {
			text = combo.getSelectedItem().toString();
		}
		
		return text;
	}


	/**
	 * Sets the currently selected text.
	 * @param text currently selected string.
	 */
	public void setText(String text) {
		combo.setSelectedItem( text );
	}


	/**
	 * Synonym for removeAllItems.
	 */
	public void clear() {
		removeAllItems();
	}



	/**
	 * Whether the (optional) radio button is selected.
	 * @return true if radio button is available and selected, otherwise false.
	 */
	public boolean isSelected() {
		if ( radioButton!=null ) return radioButton.isSelected();
		else return false;
	}
	
	/**
	 * Adds item to list. Does nothing if item is present.
	 * @param anObject object to be added to list.
	 */
	public void addItem(Object anObject) {
    if ( !hasItem(anObject) )
		combo.addItem(anObject);
	}

  /**
   * Adds all items to list.
   * @param anObject object to be added to list.
   */
	public void addAll(Collection c) {
    for (Iterator iter = c.iterator(); iter.hasNext();) 
      combo.addItem( iter.next() );
	}

	/**
	 * Removes all items from list.
	 */
	public void removeAllItems() {
		combo.removeAllItems();
	}




	/**
	 * Resets the list according to the contents of the string.
	 * @see #toPreferenceString(int)
	 * @param list of strings separated by commas.
	 */
	public void parsePreferenceString(String list) {
		clear();
		String[] hosts = list.split(",");
		for (int i = 0, n = hosts.length; i < n; i++) {
			addItem( hosts[i] );
		}
	}
	
	

	
	/**
	 * Combo box option list represented as a string.
	 * Format of String is strings separated by commas e.g. "item1,item2,item3"
	 * @param limit maximum number of items to include in string.
	 * @return string representation of first items in combo box.
	 */
	private String toPreferenceString(int limit) {
		StringBuffer buf = new StringBuffer();
		
		// add the currently selected item
		String text = getText();
		buf.append( text );
		
		// add the rest of the items so long as the number is less than the max
		int max = Math.min( combo.getItemCount(), limit-1);
		for (int i = 0; i < max; i++) {
			Object item = combo.getItemAt(i);
			// don't add duplicates items.
			if ( !text.equals(item) ) buf.append(",").append( item );
		}
		return buf.toString();
	}

	public void setPreferenceKey(String preferencesKey) {
		this.preferenceKey = preferencesKey;
	}

	public String getPreferenceKey() {
		return preferenceKey;
	}

	/**
	 * Stores the item list in preferences.
	 * @param preferences place to store items.
	 * @param limit maximum number of items from list to store.
	 * @throws RuntimeException if preferenceKey is not set.
	 */
	public void store(Preferences preferences, int limit) {

    if ( preferenceKey==null ) {
      throw new RuntimeException("PreferenceKey must be set first.");
    }
		
    String value = toPreferenceString(limit);
    if ( value!=null )
		  preferences.put(preferenceKey, value );
			 
	}
	
	
	/**
	 * Loads the item list from preferences.
	 * @param preferences object to retrieve list from.
	 * @throws RuntimeException if preferenceKey is not set.
	 */
	public void load(Preferences preferences) {
		if ( preferenceKey==null ) {
				throw new RuntimeException("PreferenceKey must be set first.");
			}
	
		parsePreferenceString( preferences.get( preferenceKey, "" ));		
	}
	/**
   * Determines whether combo box is editable by User. If not it behaves like a 
   * drop down list.
	 * @param editabale whether the user can enter text in the combo box.
	 */
	public void setEditable(boolean editable) {
		combo.setEditable( editable );
	}

	/**
	 * @return selected item.
	 */
	public Object getSelectedItem() {
		return combo.getSelectedItem();
	}

	/**
	 * @param anObject
	 */
	public void setSelectedItem(Object anObject) {
    if ( combo.getSelectedItem()==anObject) return;
		combo.setSelectedItem(anObject);
	}

	/**
	 * @param item
	 * @return true if item in combo list, otherwise false
	 */
	public boolean hasItem(Object item) {
    return indexOfItem(item)!=-1;

	}

	/**
	 * @param item
	 * @return index of item or -1 if not found
	 */
	public int indexOfItem(Object item) {
    int index = -1;
    ComboBoxModel model = combo.getModel();
    for (int i = 0; index==-1 && i < model.getSize(); i++) {
      if ( model.getElementAt(i).equals(item)  ) index = i;
    }
    return index;
	}

	/**
	 * @param index
	 * @return
	 */
	public Object getItemAt(int index) {
		return combo.getItemAt(index);
	}

	/**
	 * @return
	 */
	public int getItemCount() {
		return combo.getItemCount();
	}

}
