
package org.ensembl.mart.explorer;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;
import javax.swing.JPanel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.Field;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.QueryListener;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Base class for user input pages. Uses a BorderLayout by default.
 * 
 * <p>Includes empty implementations of the QueryChangeListeners which can be
 * overridden by implemting classes wishing to respond to query change events.
 * </p>
 */
public class InputPage extends JPanel implements QueryListener {

  protected QueryTreeView tree;

  private Object userObject;

  protected Query query;
  private Field fieldName;
  
  private String nodeLabel;  
  private MutableTreeNode node;
  private String defaultNodeLabel;

  protected List leafWidgets;  

  public InputPage(Query query, String name, QueryTreeView tree) {
    setName(name);
    this.query = query;
    this.tree = tree;
    setNodeLabel(name, null );
    node = new DefaultMutableTreeNode(this);
    leafWidgets = new ArrayList();
     
    //  use border layout so that a single item added to InputPage fills all available space in panel
    setLayout(new BorderLayout());   
    query.addQueryChangeListener(this);  

  }

  public InputPage(Query query, String name) {
    this(query, name, null); 
  }

  /**
   * Adds this instance as a query change listener.
   * @param query
   */
  public InputPage(Query query) {
    this(query, null, null); 
  }

  public MutableTreeNode getNode() {
    return node;
  }

  /**
   * Derived classes should change _label_ to
   * whatever html string they want to appear in the tree config.
   * @return label.
   */
  public String toString() {
    return nodeLabel;
  }

  public void setNodeLabel(String title, String description) {
    StringBuffer buf = new StringBuffer();
    buf.append("<html>");
    if ( title!=null ) {
      buf.append("<b>").append(title);
      if ( description!=null ) buf.append(":");
      buf.append("</b> ");
    } 
    if ( description!=null ) buf.append(description);
    buf.append( "</html>" );
    nodeLabel =  buf.toString();
  }

	/**
	 * @return
	 */
	public String getNodeLabel() {
		return nodeLabel;
	}

  /**
   * User object is an object that the user has attached to 
   * this page.
   * @return user object if set, otherwise null.
   */
  public Object getUserObject() {
    return userObject;
  }

  /**
   * User object is an object that the user has attached to 
   * this page.
   * @return user object if set, otherwise null.
   */
  public void setUserObject( Object userObject ) {
    this.userObject = userObject;
  }



  public List getLeafWidgets() {
    return leafWidgets;
  }

  /**
   * @return field created by this InputPage, null if
   * none set.
   */
  public Field getField() {
    return fieldName;
  }
  
  /**
   * @param field
   */
  protected void setField(Field field) {
    this.fieldName = field;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#queryNameChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void queryNameChanged(Query sourceQuery, String oldName, String newName) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#datasetChanged(org.ensembl.mart.lib.Query, java.lang.String, java.lang.String)
   */
  public void datasetChanged(Query source, String oldDataset, String newDataset) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#datasourceChanged(org.ensembl.mart.lib.Query, javax.sql.DataSource, javax.sql.DataSource)
   */
  public void datasourceChanged(Query sourceQuery, DataSource oldDatasource, DataSource newDatasource) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#attributeAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
   */
  public void attributeAdded(Query sourceQuery, int index, Attribute attribute) {
  }
  
  public void sequenceDescritpionChanged(Query sourceQuery, SequenceDescription seq, SequenceDescription mseq) {
  }
  

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#attributeRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
   */
  public void attributeRemoved(Query sourceQuery, int index, Attribute attribute) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#filterAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterAdded(Query sourceQuery, int index, Filter filter) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#filterRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterRemoved(Query sourceQuery, int index, Filter filter) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#filterChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter, org.ensembl.mart.lib.Filter)
   */
  public void filterChanged(Query sourceQuery, int index, Filter oldFilter, Filter newFilter) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#sequenceDescriptionChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.SequenceDescription, org.ensembl.mart.lib.SequenceDescription)
   */
  public void sequenceDescriptionChanged(Query sourceQuery, SequenceDescription oldSequenceDescription, SequenceDescription newSequenceDescription) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#limitChanged(org.ensembl.mart.lib.Query, int, int)
   */
  public void limitChanged(Query query, int oldLimit, int newLimit) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#starBasesChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void starBasesChanged(Query sourceQuery, String[] oldStarBases, String[] newStarBases) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#primaryKeysChanged(org.ensembl.mart.lib.Query, java.lang.String[], java.lang.String[])
   */
  public void primaryKeysChanged(Query sourceQuery, String[] oldPrimaryKeys, String[] newPrimaryKeys) {
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#datasetConfigChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.config.DatasetConfig, org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void datasetConfigChanged(Query query, DatasetConfig oldDatasetConfig, DatasetConfig newDatasetConfig) {
  }



}
