/*
 * Created on Aug 15, 2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package org.ensembl.mart.explorer;

import java.awt.Color;

import javax.swing.BorderFactory;
import javax.swing.JTabbedPane;

import org.ensembl.mart.lib.Query;

/**
 * Input page with a tabbed pane.
 */
public class PageWidget extends InputPage {

  /**
   * @param name
   * @param query
   */
  public PageWidget(Query query, String name, QueryTreeView tree) {
    
    super(query, name, tree);
    
    setBorder( BorderFactory.createEmptyBorder( 10, 5, 5, 5 ) );
    setBackground( Color.BLACK );
    
    tabbedPane = new JTabbedPane(  JTabbedPane.LEFT  );
    add(tabbedPane);
  }
  
  public PageWidget(Query query, String name) {
    this(query, name, null); 
  }

  protected JTabbedPane tabbedPane;

}
