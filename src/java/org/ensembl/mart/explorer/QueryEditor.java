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

import java.awt.BorderLayout;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.ensembl.mart.guiutils.PreviewPaneOutputStream;
import org.ensembl.mart.guiutils.QuickFrame;
import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.Engine;
import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.QueryAdaptor;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.CompositeDSConfigAdaptor;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.URLDSConfigAdaptor;
import org.ensembl.mart.shell.MartShellLib;
import org.ensembl.mart.util.LoggingUtil;

/**
 * Widget for creating, loading, saving and editing Queries.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class QueryEditor extends JPanel {

  /** run engine.execute(...) */
  private static final int EXECUTE = 0;
  /** run engine.countFocus(...) */
  private static final int COUNT_FOCUS = 2;

  private int preconfigLimit = 1000;
  private int maxPreconfigBytes = 100000;

  private AdaptorManager adaptorManager;

  private QueryEditorContext editorManager;

  private OutputStream os = null;

  private boolean running = false;
  private List listeners = new ArrayList();
  private ChangeEvent changeEvent = new ChangeEvent(this);

  private static final Logger logger =
    Logger.getLogger(QueryEditor.class.getName());

  /** default percentage of total width allocated to the tree constituent component. */
  private double TREE_WIDTH = 0.27d;

  /** default percentage of total height allocated to the tree constituent component. */
  private double TREE_HEIGHT = 0.7d;

  private Dimension MINIMUM_SIZE = new Dimension(50, 50);

  /** The query part of the model. */
  private Query query;

  private Engine engine = new Engine();

  private JFileChooser mqlFileChooser = new JFileChooser();

  private AdaptorManager datasetPage;
  private String currentDatasetName;
  private OutputSettingsPage outputSettingsPage;

  private AttributePageSetWidget attributesPage;
  private FilterPageSetWidget filtersPage;

  private Option lastDatasetOption;

  private Feedback feedback = new Feedback(this);

  private JFileChooser resultsFileChooser = new JFileChooser();

  private File currentDirectory;

  /** File for temporarily storing results in while this instance exists. */
  //  private File tmpFile;

  private JSplitPane leftAndRight;

  private JSplitPane middleAndBottom;

  private JEditorPane outputPanel;
  private InputPageContainer inputPanelContainer;

  /**
   * 
   * @throws IOException if fails to create temporary results file.
   */
  public QueryEditor(
    QueryEditorContext editorManager,
    AdaptorManager datasetConfigSettings)
    throws IOException {

    this.adaptorManager = datasetConfigSettings;
    this.editorManager = editorManager;
    this.query = new Query();

    // notify any state listeners when these 
    // aspects of the query are changed.
    this.query.addQueryChangeListener(new QueryAdaptor() {
      public void attributeAdded(
        Query sourceQuery,
        int index,
        Attribute attribute) {
        notifyAllListeners();
      }

      public void sequenceDescriptionChanged(
      Query sourceQuery,
	  SequenceDescription seq,
	  SequenceDescription mseq){
      	notifyAllListeners();
      }
      
      public void attributeRemoved(
        Query sourceQuery,
        int index,
        Attribute attribute) {
        notifyAllListeners();
      }

      public void datasetChanged(
        Query source,
        String oldDataset,
        String newDataset) {
        notifyAllListeners();
      }

      public void datasourceChanged(
        Query sourceQuery,
        DataSource oldDatasource,
        DataSource newDatasource) {
        notifyAllListeners();
      }

    });

    QueryTreeView treeConfig =
      new QueryTreeView(query, datasetConfigSettings.getRootAdaptor());
    inputPanelContainer =
      new InputPageContainer(query, treeConfig, datasetConfigSettings);

    outputPanel = new JEditorPane();
    outputPanel.setEditable(false);

    addWidgets(
      new JScrollPane(treeConfig),
      inputPanelContainer,
      new JScrollPane(outputPanel));

    mqlFileChooser.addChoosableFileFilter(
      new org.ensembl.gui.ExtensionFileFilter("mql", "MQL Files"));

    // set default working directory
    setCurrentDirectory(new File(System.getProperty("user.home")));

  }

  /**
   * 
   */
  protected void doClose() {
    if (editorManager != null)
      editorManager.remove(this);
  }

  /**
   * 
   */
  private void doDatasetConfigChanged() {
    // TODO Auto-generated method stub

  }

  /**
   * 
   */
  public void doLoadQuery() {

    if (getMqlFileChooser().showOpenDialog(this)
      != JFileChooser.APPROVE_OPTION)
      return;

    logger.fine("Previous query: " + query);

    try {

      File f = getMqlFileChooser().getSelectedFile().getAbsoluteFile();
      logger.fine("Loading MQL from file: " + f);

      BufferedReader r = new BufferedReader(new FileReader(f));
      StringBuffer buf = new StringBuffer();
      for (String line = r.readLine(); line != null; line = r.readLine())
        buf.append(line);
      r.close();

      logger.fine("Loaded MQL: " + buf.toString());

      MartShellLib msl = new MartShellLib(adaptorManager.getRootAdaptor());
      setQuery(msl.MQLtoQuery(buf.toString()));
      logger.fine("Loaded Query:" + getQuery());

    } catch (InvalidQueryException e) {
      feedback.warning(e.getMessage());
    } catch (IOException e) {
      feedback.warning(e.getMessage());
    }

  }

  /**
   * Save results to file, user must select file if no output file selected. 
   */
  public void doSaveResults() {
    runQuery(EXECUTE, true, false, 0);
  }

  /**
   * Save results to file, user must select output file.
   */
  public void doSaveResultsAs() {

    runQuery(EXECUTE, true, true, 0);
  }

  /**
   * convenience method.
   * @param label
   * @param listener
   * @return button configured with the parameters
   */
  private JButton createButton(
    String label,
    ActionListener listener,
    boolean enabled) {
    JButton b = new JButton(label);
    b.setEnabled(enabled);
    b.addActionListener(listener);
    return b;
  }

  /**
   * Repositions the dividers after the component has been resized to maintain
   * the relative size of the panes.
   */
  private void resizeSplits() {

    // must set divider by explicit values rather than
    // proportions because the proportion approach fails
    // on winxp jre 1.4 when the component is FIRST added.
    // (It does work when the component is resized).
    Dimension size = getParent().getSize();
    int treeWidth = (int) (TREE_WIDTH * size.width);
    int treeHeight = (int) ((1 - TREE_WIDTH) * size.height);
    leftAndRight.setDividerLocation(treeWidth);
    middleAndBottom.setDividerLocation(treeHeight);

    // need to do this so the component is redrawn on win xp jre 1.4
    validate();

  }

  /**
   * Sets the relative positions of the constituent components with splitters
   * where needed. Layout is:
   * <pre>
  
   * -----------------
   * left   |    right  
   * -----------------
   *      bottom
   * </pre>
   */
  private void addWidgets(
    JComponent left,
    JComponent right,
    JComponent bottom) {

    left.setMinimumSize(MINIMUM_SIZE);
    right.setMinimumSize(MINIMUM_SIZE);
    bottom.setMinimumSize(MINIMUM_SIZE);

    leftAndRight = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
    leftAndRight.setOneTouchExpandable(true);

    middleAndBottom =
      new JSplitPane(JSplitPane.VERTICAL_SPLIT, leftAndRight, bottom);
    middleAndBottom.setOneTouchExpandable(true);

    // don't use default FlowLayout manager because it won't resize components if
    // QueryEditor is resized.
    setLayout(new BorderLayout());

    addComponentListener(new ComponentAdapter() {
      public void componentResized(ComponentEvent e) {
        resizeSplits();
      }

    });
    add(middleAndBottom, BorderLayout.CENTER);

  }

  /**
   * Loads dataset configs from files in classpath for test
   * purposes.
   * @return preloaded dataset configs
   * @throws ConfigurationException
   */
  static DSConfigAdaptor testDSConfigAdaptor(DSConfigAdaptor adaptor)
    throws ConfigurationException {

    //TODO: change to defaultMartRegistry.xml
    //CompositeDSConfigAdaptor adaptor = new CompositeDSConfigAdaptor();

    String[] urls = new String[] {
      //"data/XML/hsapiens_gene_est.xml"
      "data/XML/hsapiens_gene_ensembl.xml"
      //,"data/XML/hsapiens_gene_vega.xml" 
    };
    for (int i = 0; i < urls.length; i++) {
      URL dvURL = QueryEditor.class.getClassLoader().getResource(urls[i]);
      //dont ignore cache, dont include hidden members (these are only for MartEditor)
      ((CompositeDSConfigAdaptor) adaptor).add(
        new URLDSConfigAdaptor(dvURL, false, false));
    }

    return adaptor;
  }

  /**
   * @return list of datasources
   */
  
  /**
  static List testDatasources() throws ConfigurationException {
    Vector dss = new Vector();
    dss.add(
      new DetailedDataSource(
        "mysql",
        "ensembldb.ensembl.org",
        "3306",
        "ensembl_mart_17_1",
        "ensembl_mart_17_1",
        "anonymous",
        null,
        10,
        "com.mysql.jdbc.Driver"));
    dss.add(
      new DetailedDataSource(
        "mysql",
        "ensembldb.ensembl.org",
        "3306",
        "ensembl_mart_18_1",
		
        "anonymous",
        null,
        10,
        "com.mysql.jdbc.Driver"));
    return dss;
  }
*/
  public static void main(String[] args) throws Exception {

    // enable logging messages
    LoggingUtil.setAllRootHandlerLevelsToFinest();
    logger.setLevel(Level.FINEST);
    Logger.getLogger(Query.class.getName()).setLevel(Level.FINEST);

    AdaptorManager dvs = testDatasetConfigSettings();
    final QueryEditor editor = new QueryEditor(null, dvs);
    editor.setName("test_query");
    editor.setPreferredSize(new Dimension(1024, 768));

    Box p = Box.createVerticalBox();
    p.add(editor);
    new QuickFrame("Query Editor (Test Frame)", p);

    // set 1st dsv to save having to do it while testing.
    editor.getQuery().setDatasetConfig(
      (DatasetConfig) dvs.getRootAdaptor().getDatasetConfigs().next());
  }

  /**
   * @return query associated with this editor.
   */
  public Query getQuery() {
    return query;
  }

  /**
   * Executes query and writes results to preconfig panel, limits number
   * of result rows printed to preconfigLimit.
   *
   */
  public void doPreview() {
    runQuery(EXECUTE, false, false, preconfigLimit);
  }

  /**
   * Executes query and writes results to temporary file.
   *
   */
  public void doExecute() {
    runQuery(EXECUTE, false, false, 0);
  }

  /**
   * Counts the focus objects that would be returned if the
   * query were executed and prints the value in the
   * preview window.
   *
   */
  public void doCountFocus() {
    runQuery(COUNT_FOCUS, false, false, 0);
  }

  /**
   * Stops running query. Does nothing if query not running.
   *
   */
  public void doStop() {

    // Stop the query by closing the output stream. This is a little hacky 
    // but is the only way to
    // stop the execution (other than Thread.stop() ) becuase the engine does not 
    // have any "stop" hooks.

    if (os != null) {
      try {
        os.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      os = null;
    }

    outputPanel.setText("");
  }

  /**
   * Executes the query on a separate thread and 
   * stores the results in a file if necesary.
   * 
   * @param method method with which to run the query, EXECUTE,
   * COUNT_ROWS or COUNT_FOCUS
   * @param save whether results file should be saved
   * @param changeResultsFile if true the results file chooser is displayed
   * @param limit max number of rows in result set
   * @see #EXECUTE
   * @see #COUNT_ROWS
   * @see #COUNT_FOCUS
   */
  private void runQuery(
    final int method,
    final boolean save,
    final boolean changeResultsFile,
    final int limit) {

    if (os != null) {
      feedback.info("Query is already running.");
      return;
    }

    //  user select result file if necessary
    if (save
      && resultsFileChooser.getSelectedFile() == null
      || changeResultsFile) {

      if (resultsFileChooser.getSelectedFile() == null)
        resultsFileChooser.setSelectedFile(new File(getName() + ".mart"));

      int option = resultsFileChooser.showSaveDialog(this);
      if (option != JFileChooser.APPROVE_OPTION)
        return;

    }

    // clear last results set before executing query
    outputPanel.setText("");

    new Thread() {

      public void run() {
        execute(method, save, limit);
      }
    }
    .start();

  }

  /**
   * Runs the query using the specified method, writes
   * results to the preview panel and saves the results 
   * to file if required.
   *
   * @param method method with which to run the query, EXECUTE,
   * COUNT_ROWS or COUNT_FOCUS
   * @param save whether results file should be saved
   * @param limit max number of rows in result set
   * @see #EXECUTE
   * @see #COUNT_ROWS
   * @see #COUNT_FOCUS   
   */
  private synchronized void execute(
    final int method,
    final boolean save,
    final int limit) {

    if (query.getDataSource() == null) {
      feedback.warning("Data base must be set before executing query.");
      return;
    } else if (
      query.getAttributes().length == 0
        && query.getSequenceDescription() == null) {
      feedback.warning("Attributes must be set before executing query.");
      return;
    }

    try {
      File outFile = null;

      if (save) {
        outFile = resultsFileChooser.getSelectedFile();
        os = new FileOutputStream(outFile);
      } else
        os = new PreviewPaneOutputStream(null, outputPanel, maxPreconfigBytes);

      int oldLimit = query.getLimit();
      if (limit > 0)
        query.setLimit(limit);

      setRunning(true);

      switch (method) {

        case EXECUTE :
          engine.execute(
            query,
            inputPanelContainer.getOutputSettingsPage().getFormat(),
            os);
          break;

        case COUNT_FOCUS :
          engine.countFocus(os, query);
          break;

        default :
          throw new RuntimeException("Unsupported method: " + method);
      }

      os.close();
      os = null;

      if (save
        && outFile != null
        && outFile.toURL().openConnection().getContentLength() < 1)
        feedback.warning("Empty result set.");

      if (limit > 0)
        query.setLimit(oldLimit);

    } catch (Exception e) {
      // if the os is null then it must have been set by doCancel() 
      e.printStackTrace();
      if (os != null) {
        try {
            os.close();
        } catch (IOException e1) {
            //ignore, it gets nulled out next
        }
        os = null;
        feedback.warning(e);
      }
    } finally {
      setRunning(false);
    }

  }

  /**
   * Set the name for this widget and the query it contains.
   */
  public void setName(String name) {
    super.setName(name);
    query.setQueryName(name);
  }

  /*
   * @return the name of this widget, this is derived from it's query.name.
   */
  public String getName() {
    return query.getQueryName();
  }

  /**
   * @return mql representation of the current query,
   * or null if datasetConfig unset.
   */
  public String getQueryAsMQL() throws InvalidQueryException {

    String mql = null;
    DatasetConfig datasetConfig = query.getDatasetConfig();
    if (datasetConfig == null)
      throw new InvalidQueryException("DatasetConfig must be selected before query can be converted to MQL.");

    MartShellLib msl = new MartShellLib(null);
    mql = msl.QueryToMQL(query, datasetConfig);

    return mql;
  }

  /**
   * Exports mql to a file chosen by user.
   */
  public void doSaveQuery() {

    try {

      String mql = getQueryAsMQL();

      if (getMqlFileChooser().showSaveDialog(this)
        != JFileChooser.APPROVE_OPTION)
        return;

      File f = getMqlFileChooser().getSelectedFile().getAbsoluteFile();
      FileOutputStream os;
      os = new FileOutputStream(f);
      os.write(mql.getBytes());
      os.write('\n');
      os.close();

    } catch (InvalidQueryException e) {
      feedback.warning(e.getMessage());
    } catch (IOException e) {
      feedback.warning(e.getMessage());
    }
  }

  /**
   * Initialises "current dir" of chooser if no file currently set. 
   */
  private JFileChooser getMqlFileChooser() {

    // Do this here rather than (more simply) in constructor because 
    // that cause an excption to be thrown on linux. Possibly a bug in JVM/library. 

    if (mqlFileChooser.getSelectedFile() == null)
      mqlFileChooser.setCurrentDirectory(currentDirectory);

    return mqlFileChooser;
  }

  /**
   * Initialise this editor with the contents of the specified query.
   * @param query query settings to be used by editor.
   */
  public void setQuery(Query query) {
    this.query.initialise(query);
  }

  /**
   * @return current directory. 
   */
  public File getCurrentDirectory() {
    return currentDirectory;
  }

  /**
   * @param directory
   * @throws IllegalArgumentException if directory not exist or is not a real direcory
   */
  public void setCurrentDirectory(File directory) {
    if (!directory.exists())
      throw new IllegalArgumentException("Directory not exist: " + directory);
    if (!directory.isDirectory())
      throw new IllegalArgumentException(
        "File is not a directory: " + directory);
    currentDirectory = directory;
  }

  /**
   * 
   */
  public static AdaptorManager testDatasetConfigSettings() {
    AdaptorManager dvs = new AdaptorManager(false);
    dvs.setAdvancedOptionsEnabled(true);
    try {
      testDSConfigAdaptor(dvs.getRootAdaptor());
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
    return dvs;

  }

  /**
   * When doPreconfig() is called a limit is set on the query with this value.
   * @return max number of rows in preconfig results.
   */
  public int getPreconfigLimit() {
    return preconfigLimit;
  }

  /**
   * Set the maximum number of rows to be displayed during a doPreconfig() cal.
   * @return max number of rows to include in preconfig results pane.
   */
  public void setPreconfigLimit(int i) {
    preconfigLimit = i;
  }

  /**
   * 
   */
  public void openDatasetConfigMenu() {
    inputPanelContainer.openDatasetConfigMenu();
  }

  /**
   * Set the cursor to busy for this component and notify 
   * listeners that the QueryEdotor is running a query or has 
   * finished running one.
   * @param running whether the query is running or not
   */
  private void setRunning(boolean running) {
    this.running = running;
    if (running)
      setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    else
      setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    notifyAllListeners();

  }

  public boolean isRunning() {
    return running;
  }

  public void addChangeListener(ChangeListener listener) {
    listeners.add(listener);
  }

  public void removeChangeListener(ChangeListener listener) {
    listeners.remove(listener);
  }

  /**
   * Notify all listeners of a state change.
   */
  private void notifyAllListeners() {
    for (Iterator iter = listeners.iterator(); iter.hasNext();)
       ((ChangeListener) iter.next()).stateChanged(changeEvent);

  }

}
