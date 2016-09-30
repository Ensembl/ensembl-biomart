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

package org.ensembl.mart.guiutils;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.ensembl.mart.editor.LabelledComboBox;

/**
 * Stateful dialog for entering and editing database settings.
 */
public class DatabaseSettingsDialog extends Box implements ChangeListener {

	
	
	private Preferences preferences;
 
	
  private LabelledComboBox databaseType;
  private LabelledComboBox driver;
	private LabelledComboBox host;
	private LabelledComboBox port;
	private LabelledComboBox database;
	private LabelledComboBox schema;
	private LabelledComboBox user;
	private LabelledComboBox martUser;
  private LabelledComboBox connectionName;
  
	private JPasswordField password;
  private String passwordPreferenceKey;
  private JCheckBox rememberPassword;
  private String rememberPasswordKey;
  
  private final String[] defaultDBTypes = new String[] { "mysql",
          "oracle",
          "postgres"
  };
  
  /* 
   * the order of the default drivers must coincide with the order of their corresponding
   * default types above for the auto completion to work
   */
  private final String[] defaultDBDrivers = new String[] { "com.mysql.jdbc.Driver",
          "oracle.jdbc.driver.OracleDriver",
          "org.postgresql.Driver"
          
  };
  
	public DatabaseSettingsDialog() {
		this(null);
	}

	public DatabaseSettingsDialog(Preferences preferences) {
	    
	    super( BoxLayout.Y_AXIS );
	 
	    connectionName = new LabelledComboBox("Display Name");
	    connectionName.setPreferenceKey("connection_name");
	    	    	    
	    add( connectionName );
	    
	    databaseType = new LabelledComboBox("Database Type");
	    databaseType.setPreferenceKey("database_type");
	    //databaseType.setEditable( false );
	    add( databaseType );
	    
	    driver = new LabelledComboBox("Database Driver");
	    driver.setPreferenceKey("driver_type");
	    //driver.setEditable( false );
	    add( driver );
	    
	    host = new LabelledComboBox("Host");
	    host.setPreferenceKey("host");
	    add( host );
	    
	    port = new LabelledComboBox("Port");
	    port.setPreferenceKey( "port" );
	    add( port );
	    
	    database = new LabelledComboBox("Database");
	    database.setPreferenceKey("database");
	    add( database );
	    
	    schema = new LabelledComboBox("Schema");
	    schema.setPreferenceKey("schema");
	    add( schema );
	    
	    
	    user = new LabelledComboBox("Database User");
	    user.setPreferenceKey("user");
	    add( user );	
	    
	    
	    add( createPasswordPanel() );
	    
	    
	    martUser = new LabelledComboBox("Mart User");
		martUser.setPreferenceKey("martUser");
		//add( martUser );	
	    
		
	    
	    if ( preferences!=null ) setPrefs(preferences);
	    
	    //setup changeListeners after initialization
	    connectionName.addChangeListener( this );
	    databaseType.addChangeListener( this );	    
	}

	/**
   * Constructs password panel.
	 * @return
	 */
	private Box createPasswordPanel() {

		password = new JPasswordField("Password");
		passwordPreferenceKey = "password";

		rememberPassword = new JCheckBox("Remember Password", false);
		rememberPasswordKey = "store_password";
		rememberPassword.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {

				if (rememberPassword.isSelected()) {

					int option =
						JOptionPane.showOptionDialog(
							rememberPassword,
							"The password will be stored unencrypted."
								+ "\nAnyone with access to your account"
								+ "\ncould read this password. "
								+ "\nRemember it?",
							"Remember Password?",
							JOptionPane.YES_NO_OPTION,
							JOptionPane.QUESTION_MESSAGE,
							null,
					    null,
							null);
					rememberPassword.setSelected(option == JOptionPane.YES_OPTION);
				}

			}
		});

		Box box = Box.createHorizontalBox();
		box.add(new JLabel("Password"));
		box.add(password);
		box.add(rememberPassword);

		return box;
	}

	private final String[] dialogOptions = new String[] { "Ok",
	                                                      "Cancel",
	                                                      "Delete"
	};
	
	public boolean showDialog(Component parent,String title) {
	    
	    if (title.equals(""))
	    	title = "Database Connection Settings";
		int option =
			JOptionPane.showOptionDialog(
				parent,
				this,
				title,
				JOptionPane.DEFAULT_OPTION,
				JOptionPane.INFORMATION_MESSAGE,
				null,
				dialogOptions,
				null);

		if (option == 2) {
		    //remove connection and its associated parameters from the persistence store
		    String cname = connectionName.getText();

		    String[] oldConnectionList = preferences.get(connectionName.getPreferenceKey(), "").split(",");

		    StringBuffer newList = new StringBuffer();
		    String comma = null;
		    
		    for (int i = 0, n = oldConnectionList.length; i < n; i++) {
                if (oldConnectionList[i].length() < 1 || oldConnectionList[i].equals(cname))  continue;
                
                if (comma != null)
                  newList.append(comma);
                
                newList.append(oldConnectionList[i]);
                comma = ",";
            }

		    if (newList.length() > 1) {
              preferences.put(connectionName.getPreferenceKey(), newList.toString());
		    }
		    
		    try {
                preferences.node(cname).clear();
                preferences.flush();
            } catch (BackingStoreException e) {
                e.printStackTrace();
            }
		}
		
		if (option != 0) {
		    loadPreferences(preferences);
			return false;
		}

		String pass = getPassword();
        storePreferences( preferences );        
        loadPreferences(preferences);
        
        //this is required to preserve password for this session only
        if (!rememberPassword.isSelected())
          password.setText(pass);
		return true;

	}



	/**
	 * 
	 */
	private void storePreferences( Preferences preferences) {

		//  persist state for next time program runs	    
	    String cname = connectionName.getText();
	    
	    connectionName.store(preferences, 10);
	    
	    if (cname != null)
          saveStoredPreferencesFor(cname);
	}


	private void loadPreferences(Preferences preferences) {
	    connectionName.load(preferences);
	    
	    String cname = connectionName.getText();
	    
	    if (cname != null)
          loadStoredPreferencesFor(cname);
	    
	    loadDBSettings();
	}

	private void loadStoredPreferencesFor(String cname) {
	      Preferences newPrefs = preferences.node(cname);
          databaseType.load(newPrefs); 
          driver.load(newPrefs);
          host.load(newPrefs);
          port.load(newPrefs);
          database.load(newPrefs);
          user.load(newPrefs);
		  martUser.load(newPrefs);
          schema.load(newPrefs);
    
          rememberPassword.setSelected( newPrefs.getBoolean( rememberPasswordKey, false) );
		  password.setText(newPrefs.get(passwordPreferenceKey, ""));
	}

	public void saveStoredPreferencesFor(String cname) {	      
	      Preferences newPrefs = preferences.node(cname);
		  databaseType.store(newPrefs, 10);
		  driver.store(newPrefs, 10);
		  host.store(newPrefs, 10);
		  port.store(newPrefs, 10);
		  database.store(newPrefs, 10);
		  schema.store(newPrefs, 10);
	      user.store(newPrefs, 10);
		  martUser.store(newPrefs, 10);	
		    
		  newPrefs.putBoolean( rememberPasswordKey, rememberPassword.isSelected() );

		  if (rememberPassword.isSelected())
		    newPrefs.put(passwordPreferenceKey, getPassword());
		  else
	        newPrefs.remove(passwordPreferenceKey);

	      try {

			// write preferences to persistent storage
			preferences.flush();

		  } catch (BackingStoreException e) {
			e.printStackTrace();
		  }
	}
	
	private void loadDBSettings() {
	    for (int i = 0, n = defaultDBTypes.length; i < n; i++) {
            if (!databaseType.hasItem( defaultDBTypes[i]))
              databaseType.addItem( defaultDBTypes[i] );
        }
	    
	    for (int i = 0, n = defaultDBDrivers.length; i < n; i++) {
          if (!driver.hasItem( defaultDBDrivers[i] ))
              driver.addItem( defaultDBDrivers[i] );
        }
	}
	
	public static void main(String[] args) {
		
		DatabaseSettingsDialog d = new DatabaseSettingsDialog( );
		d.setPrefs( Preferences.userNodeForPackage( d.getClass() ) );
		d.showDialog( null,"" );
		System.exit(0);
	}

	

  public String getConnectionName() {
    return connectionName.getText();
  }
  
	public String getHost() {
		return host.getText();
	}
	
	public String getMartUser() {
		return martUser.getText();
	}	

  public String getDatabaseType() {
    return databaseType.getText();
  }

	public void setPrefs(Preferences prefs) {
		
		this.preferences = prefs;
		
    loadPreferences( preferences );
	
	}
 
	public Preferences getPrefs() {
		return preferences;
	}


	
	public String getDatabase() {
		return database.getText();
	}

	public String getSchema() {
		return schema.getText();
	}
	
	
	
	public String getPassword() {
		return new String( password.getPassword() );
	}

	public String getPort() {
		return port.getText();
	}

	public String getUser() {
		return user.getText();
	}

	public String getDriver() {
		return driver.getText();
	}

	/**
   * Adds item if not already in list.
   * @param type
	 */
	public void addDatabaseType(String type) {
		databaseType.addItem( type );
	}

  /**
   * Adds item if not already in list.
   * @param driverName
   */
  public void addDriver(String driverName) {
    driver.addItem( driverName );
  }

  /* (non-Javadoc)
   * @see javax.swing.event.ChangeListener#stateChanged(javax.swing.event.ChangeEvent)
   */
  public void stateChanged(ChangeEvent e) {
      if (e.getSource().equals(connectionName)) {
        //thrown when connectonName box status is changed
        String cname = connectionName.getText();
      
        if (cname != null) {
          loadStoredPreferencesFor(cname);
        }
        loadDBSettings();
      } else if (e.getSource().equals(databaseType)){
          //thrown when databaseType box status is changed
          String dbtype = databaseType.getText();
          for (int i = 0, n = defaultDBTypes.length; i < n; i++) {
            if (dbtype.equals(defaultDBTypes[i])) {
                driver.setSelectedItem( defaultDBDrivers[i] );
                break;
            }
        }
      }
  }
}
