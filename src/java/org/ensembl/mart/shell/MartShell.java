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
		Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

package org.ensembl.mart.shell;

import gnu.getopt.Getopt;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.Engine;
import org.ensembl.mart.lib.FormatException;
import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.InputSourceUtil;
import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.LoggingUtils;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.SequenceException;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.URLDSConfigAdaptor;
//import org.gnu.readline.Readline;
//import org.gnu.readline.ReadlineLibrary;
import jline.*;
//import java.util.*;
/**
 * <p>Interface to a Mart Database implimentation that provides commandline access using a SQL-like query language (see MartShellLib for a 
 *  description of the Mart Query Language). The system can be used to run script files containing valid Mart Query Language commands, 
 *  or individual queries from the commandline.
 *  It has an interactive shell as well.  Script files can include comment lines beginning with #, which are ignored by the system.</p>  
 * 
 * <p>The interactive shell makes use of the <a href="http://java-readline.sourceforge.net/">Java Readline Library</a>
 * to allow commandline editing, history, and tab completion for those users working on Linux/Unix operating systems.  Unfortunately, there is no way
 * to provide this functionality in a portable way across OS platforms.  For windows users, there is a Getline c library which is provided with the Java Readline source.
 * By following the instructions to build a windows version of this library, you will get some (but not all) of this functionality.</p>
 * <p> One other side effect of the use of this library is that, because it uses GNU Readline, which is GPL, it makes MartShell GPL as well (despite the LPGL license that it and the rest
 * of Mart-Explorer are released under).  If you are serious about extending/using the MartShell class in your own code for distribution, and are worried about
 * the effects of the GPL, then you might consider rebuilding the Java Readline Library using the LGPL EditLine library, which is available on some Linux platforms.</p>
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see MartShellLib
 * @see MartCompleter
 */
public class MartShell {

  // main variables
  //private final static String REGISTRY_FILE_NAME = ".martj_adaptors.xml";
  private final static String REGISTRY_FILE_NAME = ".userMartRegistry.xml";
  

  /**
   * Default registry file loaded at startup if none
   * is found in the user's home directory.
   */
  private final static String DEFAULT_REGISTRY_URL = "data/defaultMartRegistry.xml";
  private static final String INITSCRIPT = "initScript";

  private static final String defaultConf = System.getProperty("user.home") + "/.martshell";
  private static String COMMAND_LINE_SWITCHES = "h:AR:I:M:d:vl:e:O:F:S:E:";
  private static String confinUse = null;
  private static String mainRegistry = null;
  private static String mainInitScript = null;

  private static String mainDefaultDataset = null;
  private static boolean mainBatchMode = false; // if -e is passed, true
  private static String mainBatchSQL = null;
  private static String mainBatchScriptFile = null;
  // can hold the URL to a mart script
  private static String mainBatchFile = null;
  private static String mainBatchFormat = null;
  private static String mainBatchSeparator = null;
  private static String helpCommand = null;
  private static Logger mainLogger = Logger.getLogger(MartShell.class.getName());
  
  
  /////// New Declarations for autocompletion
  public ConsoleReader myreader;
  public History hisObj;
  public List completors;
  public SimpleCompletor objSC;
  //////
  

  /**
   *  @return application usage instructions
   * 
   */
  public static String usage() {
    return "MartShell <OPTIONS>"
      + "\n"
      + "\n-h <command>                            - this screen, or, if a command is provided, help for that command"
      + "\n-A                                      - Turn off Commandline Completion (faster startup, less helpful)"
      + "\n-R MARTREGISTRY_FILE_URL                - URL or path to MartRegistry (Bookmark) document"
      + "\n-M SHELL_CONFIGURATION_FILE_URL         - URL or path to shell configuration file"
      + "\n-I INITIALIZATION_SCRIPT                - URL or path to Shell initialization MQL script"
      + "\n-d DATASETCONFIG                          - DatasetConfigname"
      + "\n-v                                      - verbose logging output"
      + "\n-l LOGGING_CONFIGURATION_URL            - URL to Java logging system configuration file (example file:data/exampleLoggingConfig.properties)"
      + "\n-e MARTQUERY                            - a well formatted Mart Query to run in Batch Mode"
      + "\n\nThe following can be used in combination with the -e or -E flag:"
      + "\n-O OUTPUT_FILE                          - output file, default is standard out"
      + "\n-F OUTPUT_FORMAT                        - output format, either tabulated or fasta"
      + "\n-S OUTPUT_SEPARATOR                     - if OUTPUT_FORMAT is tabulated, can define a separator, defaults to tab separated"
      + "\n\n-E QUERY_FILE_FILE_URL                - URL or path to file with valid Mart Query Commands"
      + "\n\nThe application searches for a .martshell file in the user home directory for shell configuration information."
      + "\nif present, this file will be loaded. If the -M, -R or -I options are given, these over-ride those values provided in the .martshell file"
      + "\nUsers specifying a shell configuration file with -M,"
      + "\nor using a .martshell file, can use -R, or -I to specify"
      + "\nparameters not specified in the configuration file, or over-ride those that are specified."
      + "\n\nAn Inititialization script can contain any MQL statements, but is best suited to statements concerning"
      + "\nMart management, such as initializing the Mart to query for the session, or the various DatasetConfigs"
      + "\nbeing querried."
      + "\n";
  }

  /**
   * Parses java properties file to get mysql shell configuration parameters.
   * 
   * @param connfile -- String name of the configuration file containing shell
   * configuration properties.
   */
  public static void getConnProperties(String connfile) {
    Properties p = new Properties();

    try {
      p.load(InputSourceUtil.getStreamForString(connfile));

      String tmp = p.getProperty(INITSCRIPT);
      if (tmp != null && tmp.length() > 1 && mainInitScript == null)
        mainInitScript = tmp.trim();
    } catch (java.net.MalformedURLException e) {
      mainLogger.warning("Could not load connection file " + connfile + " MalformedURLException: " + e);
    } catch (java.io.IOException e) {
      mainLogger.warning("Could not load connection file " + connfile + " IOException: " + e);
    }
    confinUse = connfile;
  }

  private static String[] harvestArguments(String[] oargs) throws Exception {
    Hashtable argtable = new Hashtable();
    String key = null;

    for (int i = 0, n = oargs.length; i < n; i++) {
      String arg = oargs[i];

      if (arg.startsWith("-")) {
        String thisArg = arg;
        key = null;
        String value = null;

        if (thisArg.length() > 2) {
          key = thisArg.substring(0, 2);
          value = thisArg.substring(2);
        } else
          key = thisArg;

        if (!argtable.containsKey(key)) {
          StringBuffer buf = new StringBuffer();

          if (value != null) {
            //strip leading and trailing quotes
            if (value.startsWith("'"))
              value = value.substring(1);
            if (value.startsWith("\""))
              value = value.substring(1);

            if (value.endsWith("'"))
              value = value.substring(0, value.lastIndexOf("'"));
            if (value.endsWith("\""))
              value = value.substring(0, value.lastIndexOf("\""));

            buf.append(value);
          }

          argtable.put(key, buf);
        }
      } else {
        if (key == null)
          throw new Exception("Invalid Arguments Passed to MartShell\n");
        StringBuffer value = (StringBuffer) argtable.get(key);
        if (value.length() > 0)
          value.append(" ");

        //strip leading and trailing quotes
        if (arg.startsWith("'"))
          arg = arg.substring(1);
        if (arg.startsWith("\""))
          arg = arg.substring(1);

        if (arg.endsWith("'"))
          arg = arg.substring(0, arg.lastIndexOf("'"));
        if (arg.endsWith("\""))
          arg = arg.substring(0, arg.lastIndexOf("\""));

        value.append(arg);
        argtable.put(key, value);
      }
    }

    String[] ret = new String[argtable.size() * 2];
    // one slot for each key, and one slot for each non null or non empty value
    int argnum = 0;

    for (Iterator iter = argtable.keySet().iterator(); iter.hasNext();) {
      String thiskey = (String) iter.next();
      String thisvalue = ((StringBuffer) argtable.get(thiskey)).toString();

      ret[argnum] = thiskey;
      argnum++;

      // getOpt wants an empty string for switches
      if (thisvalue.length() < 1)
        thisvalue = "";

      ret[argnum] = thisvalue;
      argnum++;
    }

    return ret;
  }

  public static void main(String[] oargs) {

    String loggingURL = null;
    boolean help = false;
    boolean verbose = false;
    boolean commandComp = true;

    // check for the defaultConf file, and use it, if present.  Some values may be overridden with a user specified file with -g
    if (new File(defaultConf).exists())
      getConnProperties(defaultConf);

    String[] args = null;
    if (oargs.length > 0) {
      try {
        args = harvestArguments(oargs);
      } catch (Exception e1) {
        System.err.println(e1.getMessage());
        e1.printStackTrace();
        System.exit(1);
      }

      Getopt g = new Getopt("MartShell", args, COMMAND_LINE_SWITCHES);
      int c;

      while ((c = g.getopt()) != -1) {

        switch (c) {

          case 'h' :
            help = true;
            helpCommand = g.getOptarg();
            break;

          case 'R' :
            mainRegistry = g.getOptarg();
            break;

          case 'I' :
            mainInitScript = g.getOptarg();
            break;

          case 'A' :
            commandComp = false;
            break;

            // get everything that is specified in the provided configuration file, then fill in rest with other options, if provided
          case 'M' :
            getConnProperties(g.getOptarg());
            break;

          case 'd' :
            mainDefaultDataset = g.getOptarg();
            break;

          case 'v' :
            verbose = true;
            break;

          case 'l' :
            loggingURL = g.getOptarg();
            break;

          case 'e' :
            mainBatchSQL = g.getOptarg();
            mainBatchMode = true;
            break;

          case 'O' :
            mainBatchFile = g.getOptarg();
            break;

          case 'F' :
            mainBatchFormat = g.getOptarg();
            break;

          case 'S' :
            mainBatchSeparator = g.getOptarg();
            break;

          case 'E' :
            mainBatchScriptFile = g.getOptarg();
            mainBatchMode = true;
            break;
        }
      }
    } else {
      args = new String[0];
    }

    // Initialise logging system
    if (loggingURL != null) {
      try {
        LoggingUtils.setLoggingConfiguration(InputSourceUtil.getStreamForString(loggingURL));
      } catch (SecurityException e) {
        System.err.println("Caught Security Exception when adding logger configuration URL");
        e.printStackTrace();
        System.err.println("\n\nContinuing to load\n");
      } catch (MalformedURLException e) {
        System.err.println("User supplied URL " + loggingURL + " is not well formed");
        e.printStackTrace();
        System.err.println("\n\nContinuing to load\n");
      } catch (IOException e) {
        System.err.println("Could not read input from URL " + loggingURL + "\n");
        e.printStackTrace();
        System.err.println("\n\nContinuing to load\n");
      }
    } else {
      LoggingUtils.setVerbose(verbose);
    }

    if (confinUse != null) {
      mainLogger.info("Using configuration file: " + confinUse + "\n");
    } else {
      mainLogger.info("Using commandline options only for connection configuration");
    }

    // check for help
    if (help) {
      if (helpCommand.equals(""))
        System.err.println(usage());
      else {
        MartShell ms = new MartShell();
        ms.UnsetCommandCompletion();
        try {
          System.out.println(ms.Help(helpCommand));
        } catch (InvalidQueryException e) {
          System.err.println("Couldnt provide Help for " + helpCommand + e.getMessage());
          e.printStackTrace();
          System.exit(0);
        }
      }
      return;
    }

    if (!mainBatchMode)
      System.out.println("Starting Interactive MartShell\n");

    MartShell ms = new MartShell();

    //if user has specified -R flag, use the Registry that they specify, else, load defaults
    if (mainRegistry == null) {
      mainRegistry = DEFAULT_REGISTRY_URL;

      //if user has placed .martj_adaptors in home, use it over DEFAULT_REGISTRY_URL
      String path = System.getProperty("user.home") + File.separator + REGISTRY_FILE_NAME;
      File file = new File(path);
      if (file.exists())
        mainRegistry = path;
    }

    try {
      ms.addMartRegistry(mainRegistry);
    } catch (MalformedURLException e1) {
      System.err.println("Could not set default Registry file " + mainRegistry + "\n" + e1.getMessage());
      e1.printStackTrace();
      System.err.println("\n\nContinuing to load\n");
    } catch (ConfigurationException e1) {
      System.err.println("Could not set default Registry file " + mainRegistry + "\n" + e1.getMessage());
      e1.printStackTrace();
      System.err.println("\n\nContinuing to load\n");
    }

    if (mainInitScript != null)
      try {
        ms.initializeWithScript(mainInitScript);
      } catch (Exception e2) {
        System.err.println("Could not initialize MartShell with initScript " + e2.getMessage());
        e2.printStackTrace();
        System.err.println("\n\nContinuing to load\n");
      }

    if (mainDefaultDataset != null)
      try {
        ms.setDefaultDataset(mainDefaultDataset);
      } catch (InvalidQueryException e3) {
        System.err.println("Could not set default dataset to " + mainDefaultDataset + " " + e3.getMessage());
        e3.printStackTrace();
        System.err.println("\n\nContinuing to load\n");
      }

    if (mainBatchMode) {
      boolean validQuery = true;
      ms.UnsetCommandCompletion();

      if (mainBatchFile != null) {
        try {
          ms.setBatchOutputFile(mainBatchFile);
        } catch (Exception e) {
          validQuery = false;
        }
      }

      if (mainBatchFormat != null)
        ms.setBatchOutputFormat(mainBatchFormat);

      if (mainBatchSeparator == null)
        ms.setBatchOutputSeparator("\t"); //default
      else
        ms.setBatchOutputSeparator(mainBatchSeparator);

      if (mainBatchSQL == null && mainBatchScriptFile == null) {
        System.out.println("Must supply either a Query command or a query script\n" + usage());
        System.exit(0);
      } else if (mainBatchScriptFile != null) {
        validQuery = ms.RunBatchScript(mainBatchScriptFile);
      } else {
        validQuery = ms.RunBatch(mainBatchSQL);
      }
      if (!validQuery) {
        System.err.println("Invalid Batch command:" + ms.getBatchError() + "\n" + usage());
        System.exit(1);
      } else
        System.exit(0);
    } else {
      if (!commandComp)
        ms.UnsetCommandCompletion();

      ms.RunInteractive();
    }
  }

  public MartShell() {
    initializeMartShellLib();
  }

  /**
   * Method for creating an interactive MartShell session.  This system
   * will attempt to load the Java Readline library, first using Getline (Windows),
   * then attempting EditLine (linux/Unix LGPL), then GnuReadline (Linux GPL), 
   * and finally, given that all of these attempts have failed, loads the PureJava 
   * library, which is merely a wrapper around standard out/standard in, and provides 
   * no history/completion functionality.  The user is warned if PureJava is loaded.
   * It sets state depending on which library sucessfully loaded.  If the user
   * hasnt turned completion off with the -A flag, and a compatible library was loaded,
   * a MartCompleter is initialized, and the Readline is linked to that to allow
   * context sensitive command completion.  It then enters into the shell loop,
   * which attempts to capture all Exceptions and report them to the user at
   * the martshell prompt without exiting. 
   *
   */
  public void RunInteractive() 
  {
    continueQuery = false;
    interactiveMode = true;
 /*   
    try {
      Readline.load(ReadlineLibrary.Getline);
      //		Getline doesnt support completion, or history manipulation/files
      completionOn = false;
      historyOn = true;
      readlineLoaded = true;
    } catch (UnsatisfiedLinkError ignore_me) {
      try {
        Readline.load(ReadlineLibrary.Editline);
        historyOn = true;
        completionOn = true;
        readlineLoaded = true;
      } catch (UnsatisfiedLinkError ignore_me2) {
        try {
          Readline.load(ReadlineLibrary.GnuReadline);
          historyOn = true;
          completionOn = true;
          readlineLoaded = true;
        } catch (UnsatisfiedLinkError ignore_me3) {
          mainLogger.warning(
            "Could not load Readline Library, commandline editing, completion will not be available"
              + "\nConsult MartShell documentation for methods to resolve this error.");
          historyOn = false;
          completionOn = false;
          readlineLoaded = false;
          Readline.load(ReadlineLibrary.PureJava);
        }	
      }
    }
    //System.out.println("HELLO-BEFORE-1");
    Readline.initReadline("MartShell");
    //System.out.println("HELLO-BEFORE-2");
     */
    
    
    Runtime.getRuntime().addShutdownHook
    (
    	new Thread() {
    		public void run() {
    //			System.out.println("HELLO-BEFORE");
    //			Readline.cleanup();
    //			System.out.println("HELLO-AFTER");
    		}
    	}
    );

    try {

//////////////////////////
        historyOn = true;
        completionOn = true;
        readlineLoaded = true;
        myreader = new ConsoleReader();
    	//myreader.setDebug (new PrintWriter (new FileWriter ("writer.debug", true)));
    	completors = new LinkedList ();
    	objSC= new SimpleCompletor (new String [] { "list", "add", "remove", "set", "unset", "help", "use", "using"});
    	completors.add(objSC);
    	hisObj = new History();
    	
    	////////////////////////
    	// load help file
       	loadHelpFiles();
       		
      //display startup information
      System.out.println();
      System.out.println(supportHelp.getProperty(STARTUP));
      System.out.println();
    } catch (InvalidQueryException e2) {
      System.err.println("Couldnt display startup information\n" + e2.getMessage());

      StackTraceElement[] stacks = e2.getStackTrace();
      StringBuffer stackout = new StringBuffer();

      for (int i = 0, n = stacks.length; i < n; i++) {
        StackTraceElement element = stacks[i];
        stackout.append(element.toString()).append("\n");
      }

      if (mainLogger.isLoggable(Level.INFO))
        mainLogger.info("\n\nStackTrace:\n" + stackout.toString());
    }
    catch (IOException ie1)
    {
    	System.err.println("Couldnt display startup information\n" + ie1.getMessage());
    	System.out.println("IO EXception by Console reader where its renewed : From Code MartShell");
    }
    

    if (completionOn) {
      mcl = new MartCompleter(completors);
      try {
        mcl.setController(msl);
      } catch (ConfigurationException e) {
        System.err.println(
          "Recieved Exception Loading DatasetNames into Completer: "
            + e.getMessage()
            + "\ncontinuing without this information!\n");
      }

      // add commands
      List allCommands = new ArrayList();
      allCommands.addAll(availableCommands);
      allCommands.addAll(msl.availableCommands);
      mcl.setBaseCommands(allCommands);

      mcl.setAddCommands(addRequests);
      mcl.setRemoveBaseCommands(removeRequests);
      mcl.setListCommands(listRequests);
      mcl.setUpdateBaseCommands(updateRequests);
      mcl.setSetBaseCommands(setRequests);
      mcl.setDescribeBaseCommands(describeRequests);
      mcl.setEnvironmentBaseCommands(envRequests);
      mcl.setExecuteBaseCommands(executeRequests);

      // add sequences
      //mcl.setDomainSpecificCommands(SequenceDescription.SEQS); // will need to modify this if others are added

      if (helpLoaded)
        mcl.setHelpCommands(commandHelp.keySet());

      updateCompleter();
      mcl.setCommandMode();

     // Readline.setCompleter(mcl);
     myreader.addCompletor(this.mcl);
    }

    if (readlineLoaded && historyOn) {
/*      try {
        File histFile = new File(history_file);
        histFile.
        if (!histFile.exists())
          histFile.createNewFile();
        else
          LoadScriptFromFile(history_file);
      } catch (Exception e1) {
        if (mainLogger.isLoggable(Level.FINE))
          mainLogger.fine("Could not load history: " + e1.getMessage() + "\n continuing to load!\n");
      }
*/
    	try {
       	LoadScriptFromFile(history_file); // Loads History from a specified file if it exists
    	}
    	catch (Exception e1) {
    		if (mainLogger.isLoggable(Level.FINE))
    			mainLogger.fine("Could not load history: " + e1.getMessage() + "\n continuing to load!\n");
    	}
    }
    myreader.setHistory(hisObj);
    
    String thisline = null;
    while (true) {
      try {
        thisline = Prompt();
        
        //System.out.println("\nSHAZI: " + thisline); // comes after the enter is pressed.
        
        if (thisline != null) {
          if (thisline.equals(EXITC) || thisline.equals(QUITC))
            break;
          if (thisline.startsWith(HELPC))
            System.out.print(Help(normalizeCommand(thisline)));
          else {
            parse(thisline);
            thisline = null;
          }
        }
      } catch (Exception e) {
        if (e instanceof EOFException) {
          System.out.println();
          break;
        }

        System.err.println(e.getMessage());

        StackTraceElement[] stacks = e.getStackTrace();
        StringBuffer stackout = new StringBuffer();

        for (int i = 0, n = stacks.length; i < n; i++) {
          StackTraceElement element = stacks[i];
          stackout.append(element.toString()).append("\n");
        }

        if (mainLogger.isLoggable(Level.INFO))
          mainLogger.info("\n\nStackTrace:\n" + stackout.toString());

        conline = new StringBuffer();
        continueQuery = false;
        thisline = null;
      }
    }

    try {
      ExitShell();
    } catch (IOException e) {
      System.err.println("Warning, could not close Buffered Reader\n");
      StackTraceElement[] stacks = e.getStackTrace();
      StringBuffer stackout = new StringBuffer();

      for (int i = 0, n = stacks.length; i < n; i++) {
        StackTraceElement element = stacks[i];
        stackout.append(element.toString()).append("\n");
      }

      if (mainLogger.isLoggable(Level.INFO))
        mainLogger.info("\n\nStackTrace:\n" + stackout.toString());

      System.exit(1);
    }
  }

  /**
   * Method for running a batchScript file non-interactively.  This will attempt
   * to parse/run all of the Mart Query Languge commands in the file in succession.
   * It Returns true if the commands were executed successfully.  If there is an error,
   * it sets the BatchError message, and returns false.
   *  
   * @param batchScriptFile - String path to file to be loaded and evaluated
   * @return boolean true if all commands are executed successfully, false if not.
   */
  public boolean RunBatchScript(String batchScriptFile) {
    historyOn = false;
    completionOn = false;
    readlineLoaded = false;

    boolean valid = true;
    try {
      InputStream input = InputSourceUtil.getStreamForString(batchScriptFile);

      ExecScriptFromStream(input);
      input.close();
    } catch (Exception e) {
      setBatchError(e.getMessage());
      StackTraceElement[] stacks = e.getStackTrace();
      StringBuffer stackout = new StringBuffer();

      for (int i = 0, n = stacks.length; i < n; i++) {
        StackTraceElement element = stacks[i];
        stackout.append(element.toString()).append("\n");
      }
      mainLogger.info("\n\nStackTrace:\n" + stackout.toString());
      valid = false;
    }
    return valid;
  }

  /**
   * Method for running a single command non-interactively.  This will
   * attempt to parse/run the entire queryString provided.  It returns
   * true if the command was executed successfully, false if exceptions
   * occur (including connection exceptions, IO Exceptions, etc.).  When
   * Exceptions occur, the message is stored in the BatchError message.
   * 
   * @param querystring - String Mart Query Languate String to be evaluated
   * @return boolean true if querystring is executed successfully, false if not.
   */
  public boolean RunBatch(String querystring) {
    historyOn = false;
    completionOn = false;
    readlineLoaded = false;

    boolean validQuery = true;

    try {
      if (!querystring.endsWith(LINEEND))
        querystring = querystring + LINEEND;

      parseForCommands(querystring);
    } catch (Exception e) {
      setBatchError(e.getMessage());
      StackTraceElement[] stacks = e.getStackTrace();
      StringBuffer stackout = new StringBuffer();

      for (int i = 0, n = stacks.length; i < n; i++) {
        StackTraceElement element = stacks[i];
        stackout.append(element.toString()).append("\n");
      }

      if (mainLogger.isLoggable(Level.INFO))
        mainLogger.info("\n\nStackTrace:\n" + stackout.toString());

      validQuery = false;
    }

    return validQuery;
  }

  /**
   * Method allowing client scripts to specifically turn off command completion
   *
   */
  public void UnsetCommandCompletion() {
    completionOn = false;
  }

  public void setLoggingConfigurationURL(URL conf) {
    loggingConfURL = conf;
  }

  /**
   * Method allowing client scripts to specify a default MartRegistry.dtd compliant
   * document specifying the location of Mart DatasetConfigs.
   * 
   * @param confFile - String path or URL to MartRegistry file
   * 
   */
  public void addMartRegistry(String confFile) throws ConfigurationException, MalformedURLException {
    msl.addMartRegistry(confFile, false);
  }

  /**
   * Takes a script with MQL commands (typically, add Mart, set Mart, add DatasetConfig(s), use DatasetConfig ,etc).
   * This will ignore lines commented with #.
   * @param initScript -- either path or URL to MQL initialization script.
   */
  public void initializeWithScript(String initScript)
    throws ConfigurationException, SequenceException, FormatException, InvalidQueryException, IOException, SQLException {

    InputStream input = InputSourceUtil.getStreamForString(initScript);
    ExecScriptFromStream(input);
    input.close();
  }

  /**
   * Set the name of a file to output a batch Mart Query command using the -e flag
   * 
   * @param batchFile - String path to output file.
   * @throws IOException when the specified file cannot be created.
   */
  public void setBatchOutputFile(String batchFileName) throws IOException {
    try {
      File batchFile = new File(batchFileName);
      if (!batchFile.exists())
        batchFile.createNewFile();

      sessionOutput = new FileOutputStream(batchFile);
    } catch (FileNotFoundException e) {
      setBatchError("Could not open file " + batchFileName + "\n" + e.getMessage());
      throw e;
    }
  }

  /**
   * Set the format for output of a batch Mart Query command using the -e flag
   * 
   * @param outputFormat - String format, must be either tabulated or fasta, or an exception will be thrown when the query executes.
   */
  public void setBatchOutputFormat(String outputFormat) {
    this.sessionOutputFormat = outputFormat;
  }

  /**
   * Set the output separator for tabulated output of a batch Mart Query command
   * using the -e flag.
   * 
   * @param outputSeparator - String field separator (defaults to tab separated if none specified)
   */
  public void setBatchOutputSeparator(String outputSeparator) {
    this.sessionOutputSeparator = outputSeparator;
  }

  /**
   * sets the DatasetName to the provided String
   * @param datasetName - string internalName of the dataset
   */
  public void setDefaultDataset(String datasetName) throws InvalidQueryException {
    msl.setEnvDataset(datasetName);
  }

  /**
   * Get any error message, if the runBatch or runBatchScript command returns
   * false.
   * 
   * @return String error message
   */
  public String getBatchError() {
    return batchErrorMessage;
  }

  private void setBatchError(String message) {
    batchErrorMessage = message;
  }

  private void initializeMartShellLib() {
    if (msl == null) {
      msl = new MartShellLib();
      msl.setMaxCharCount(MAXCHARCOUNT);
    }
  }

  private void ExitShell() throws IOException {
    //Readline.cleanup();

    // if history and completion are on, save the history file
    if (readlineLoaded && historyOn)
    {
    		// Readline.writeHistoryFile(history_file); 
    	// TODO: Logic of storing history to a file using the above sort of function
    	// Nothing required as a history file gets associated automatically from
    	// LoadScriptFromFile, and gets updated automatically each time.
    }
    
    	
    
    //  ensures that newline is printed before exit
    System.out.println();
    System.out.println();
    System.out.flush();
    System.exit(0);
  }

  private String Prompt() throws EOFException, UnsupportedEncodingException, IOException {
    String line = null;
    if (continueQuery)
      line = subPrompt();
    else
      line = mainPrompt();

    return line;
  }

  private String mainPrompt() throws EOFException, UnsupportedEncodingException, IOException {
    String prompt = null;
    String line = null;

    if (userPrompt != null)
      prompt = userPrompt;
    else
      prompt = DEFAULTPROMPT + "> ";

    if (completionOn)
      mcl.setCommandMode();

    //line = Readline.readline(prompt, historyOn);
    line = myreader.readLine(prompt);
    return line;
  }

  private String subPrompt() throws EOFException, UnsupportedEncodingException, IOException {
    //return Readline.readline("% ", historyOn);
	  return myreader.readLine("% ");
  }

  private String normalizeCommand(String command) {
    String normalizedcommand = null;

    if (command.endsWith(LINEEND))
      normalizedcommand = command.substring(0, command.indexOf(LINEEND));
    else
      normalizedcommand = command;

    return normalizedcommand;
  }

  public String Help(String command) throws InvalidQueryException {
    if (!helpLoaded)
      loadHelpFiles();

    StringBuffer buf = new StringBuffer();

    if (command.equals(HELPC)) {
      buf.append("\nAvailable items:\n");

      for (Iterator iter = commandHelp.keySet().iterator(); iter.hasNext();) {
        String element = (String) iter.next();
        //mainLogger.info("ELEMENT IS <" + element + ">\nVALUE IS " + commandHelp.getProperty(element) + "\n" );
        buf.append("\t\t" + element).append("\n");
      }
      return buf.toString();

    } else if (command.startsWith(HELPC))
      return Help(command.substring(command.indexOf(HELPC) + HELPC.length() + 1).trim());
    else {
      buf.append("\n");
      StringTokenizer hToks = new StringTokenizer(command, " ");
      command = hToks.nextToken();

      if (commandHelp.containsKey(command)) {
        String output = commandHelp.getProperty(command);

        //				check for domain specific bits in the help input to substitute
        Matcher m = DOMAINSPP.matcher(output);

        while (m.find()) {
          String replacement = m.group(1);
          if (supportHelp.containsKey(replacement))
            m.appendReplacement(buf, supportHelp.getProperty(replacement));
          else
            m.appendReplacement(buf, "");
        }
        m.appendTail(buf);

        output = buf.toString();
        buf = new StringBuffer();

        // check for INSERT
        m = INSERTP.matcher(output);
        while (m.find()) {
          String replacement = m.group(1);
          if (supportHelp.containsKey(replacement))
            m.appendReplacement(buf, supportHelp.getProperty(replacement));
          else
            m.appendReplacement(buf, "");
        }
        m.appendTail(buf);
      } else
        buf.append("Sorry, no information available for item: ").append(command).append("\n");

      return buf.toString();
    }
  }

  private void loadHelpFiles() throws InvalidQueryException {
    URL help = ClassLoader.getSystemResource(HELPFILE);
    URL dshelp = ClassLoader.getSystemResource(DSHELPFILE);
    URL helpsupport = ClassLoader.getSystemResource(HELPSUPPORT);
    URL dshelpsupport = ClassLoader.getSystemResource(DSHELPSUPPORT);
    try {
      commandHelp.load(help.openStream());
      commandHelp.load(dshelp.openStream());

      supportHelp.load(helpsupport.openStream());
      supportHelp.load(dshelpsupport.openStream());

      if (!historyOn) {
        commandHelp.remove(HISTORYQ);
        commandHelp.remove(HISTORYC);
        availableCommands.remove(HISTORYC);
        commandHelp.remove(LOADSCRPTC);
        availableCommands.remove(LOADSCRPTC);
        commandHelp.remove(SAVETOSCRIPTC);
        availableCommands.remove(SAVETOSCRIPTC);
      }

      if (!completionOn)
        commandHelp.remove(COMPLETIONQ);
    } catch (IOException e) {
      helpLoaded = false;
      throw new InvalidQueryException("Could not load Help File " + e.getMessage());
    }
    helpLoaded = true;
  }

  private void pageOutput(String[] lines) {
    int linesout = 0;
    for (int i = 0, n = lines.length; i < n; i++) {
      if (interactiveMode && (linesout > MAXLINECOUNT)) {
        linesout = 0;
        try {
          //String quit = Readline.readline("\n\nHit Enter to continue, q to return to prompt: ", false);
        	String quit = myreader.readLine("\n\nHit Enter to continue, q to return to prompt: ");
          if (quit.equals("q")) {
            System.out.println();
            break;
          }

          System.out.println("\n");
        } catch (Exception e) {
          // do nothing
        }
      }
      String line = lines[i];
      System.out.print(line);
      linesout++;
    }
  }

  private void listRequest(String command) throws InvalidQueryException, ConfigurationException {
    System.out.println();
    String[] toks = command.split("\\s+");

    if (toks.length >= 2) {
      String request = toks[1];
      String[] lines = null;

      if (request.equalsIgnoreCase(DATASETCONFIGSREQ))
        lines = msl.listDatasetConfigs(toks);
      else if (request.equalsIgnoreCase(DATASETSREQ))
        lines = msl.listDatasets(toks);
      else if (request.equalsIgnoreCase(FILTERSREQ))
        lines = msl.listFilters();
      else if (request.equalsIgnoreCase(ATTRIBUTESREQ))
        lines = msl.listAttributes();
      else if (request.equalsIgnoreCase(PROCSREQ))
        lines = msl.listProcedures();
      else if (request.equalsIgnoreCase(MARTSREQ))
        lines = msl.listMarts();
      else
        throw new InvalidQueryException("Invalid list command recieved: " + command + "\n");

      if (lines != null) {
        pageOutput(lines);
      }
    } else
      throw new InvalidQueryException("Invalid list command recieved: " + command + "\n");
    System.out.println();
  }

  private void describeRequest(String command) throws InvalidQueryException, ConfigurationException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    int tokCount = toks.countTokens();
    toks.nextToken(); // skip describe

    System.out.println();

    if (tokCount < 2)
      throw new InvalidQueryException("Invalid Describe request " + command + "\n" + Help(DESCC));
    else {
      String request = toks.nextToken();
      String name = (toks.hasMoreTokens()) ? toks.nextToken() : null;

      if (request.equalsIgnoreCase(MARTREQ)) {
        String tmp = msl.DescribeMart(name);
        System.out.println(tmp + "\n");
      } else if (request.equalsIgnoreCase(DATASETREQ)) {
        String[] lines = msl.DescribeDataset(name);

        pageOutput(lines);
      } else if (request.equalsIgnoreCase(FILTERREQ)) {
        if (name == null)
          throw new InvalidQueryException("Invalid Describe filter request " + command + "\n" + Help(DESCC));

        String tmp = msl.DescribeFilter(name);
        System.out.println(tmp + "\n");
      } else if (request.equalsIgnoreCase(ATTRIBUTEREQ)) {
        if (name == null)
          throw new InvalidQueryException("Invalid Describe attribute request " + command + "\n" + Help(DESCC));

        String tmp = msl.DescribeAttribute(name);
        System.out.println(tmp + "\n");
      } else if (request.equalsIgnoreCase(PROCREQ)) {
        String out = msl.describeStoredMQLCommand(name);
        if (out == null)
          throw new InvalidQueryException("Procedure " + name + " has not been defined\n");
        else
          System.out.println(out);
      } else
        throw new InvalidQueryException("Invalid Request key in describe command, see help describe. " + command + "\n");
    }
  }

  private void addRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); // skip add

    if (toks.hasMoreTokens()) {
      String addreq = toks.nextToken();
      if (addreq.equalsIgnoreCase(MARTREQ))
        addMart(toks);
      else if (addreq.equalsIgnoreCase(DATASETSREQ))
        msl.addDatasets(toks);
      else if (addreq.equalsIgnoreCase(DATASETCONFIGREQ))
        msl.addDatasetConfig(toks);
      else
        throw new InvalidQueryException("Invalid Add request recieved " + command + "\n" + Help(ADDC) + "\n");
    } else
      throw new InvalidQueryException("Invalid Add request recieved " + command + "\n" + Help(ADDC) + "\n");

    updateCompleter();
  }

  private void addMart(StringTokenizer toks) throws InvalidQueryException {
    String martHost = null;
    String martDatabaseType = null;
    String martPort = null;
    String martUser = null;
    String martPass = null;
    String martDatabase = null;
    String martSchema = null;
    String martDriver = null;
    String sourceKey = null;

    if (toks.countTokens() > 0) {

      String connSettings = toks.nextToken();

      while (toks.hasMoreTokens())
        connSettings += " " + toks.nextToken();

      if (connSettings.indexOf(" as ") > 0) {
        String tmpC = connSettings.substring(0, connSettings.indexOf(" as ")).trim();
        sourceKey = connSettings.substring(connSettings.indexOf(" as ") + 3).trim();
        connSettings = tmpC;
      }

      connSettings = connSettings.replaceAll("\\s+", "");

      //pattern to find and parse all occurances of x=y, or x = y in a string
      Pattern pat = Pattern.compile("(\\w+\\=\\'[^\\']+\\')");
      Matcher mat = pat.matcher(connSettings);
      boolean matchFound = false;

      while (mat.find()) {
        matchFound = true;
        String addReq = mat.group();

        String[] setkv = addReq.split("\\=");

        String key = setkv[0];
        String value = setkv[1];

        value = value.substring(value.indexOf("'") + 1, value.lastIndexOf("'")); // strip off leading and trailing quotes

        if (key.equals(DBHOST))
          martHost = value;
        else if (key.equals(DATABASETYPE))
          martDatabaseType = value;
        else if (key.equals(DBPORT))
          martPort = value;
        else if (key.equals(DBUSER))
          martUser = value;
        else if (key.equals(DBPASSWORD))
          martPass = value;
        else if (key.equals(INSTANCENAME))
          martDatabase = value;
        else if (key.equals(DBDRIVER))
          martDriver = value;
        else
          throw new InvalidQueryException("Recieved invalid add Mart command.\n" + Help(ADDC) + "\n");
      }

      if (!matchFound)
        throw new InvalidQueryException("Invalid set Output request " + connSettings + "\n" + Help(ADDC));

    } else {
      if (mainBatchMode)
        throw new InvalidQueryException("Recieved invalid add Mart command.\n" + Help(ADDC) + "\n");

      String thisLine = null;

      //List oldHistory = new Vector();
      //Readline.getHistory(oldHistory);
      //Readline.clearHistory();
      
      try {
//        if (lastDBSettings[HOSTITER] != null)
 //         Readline.addToHistory(lastDBSettings[HOSTITER]);

   //     thisLine = Readline.readline("\nHost: ", false);
    	  thisLine = myreader.readLine("\nHost: ");
        if (thisLine != null)
          martHost = thisLine;

        //Readline.clearHistory();
       // if (lastDBSettings[DBTYPEITER] != null)
       //   Readline.addToHistory(lastDBSettings[DBTYPEITER]);

        //thisLine =  Readline.readline( "\nDatabase Type (default " + DetailedDataSource.DEFAULTDATABASETYPE +"): ", false);
        thisLine =  myreader.readLine( "\nDatabase Type (default " + DetailedDataSource.DEFAULTDATABASETYPE +"): ");
        if (thisLine != null)
          martDatabaseType = thisLine;

    
//      switched off interactive driver
        
        /**
        Readline.clearHistory();
        if (lastDBSettings[DRIVERITER] != null)
          Readline.addToHistory(lastDBSettings[DRIVERITER]);

        thisLine =
          Readline.readline(
            "\nDriver Class Name (default "
              + DetailedDataSource.DEFAULTDRIVER
              +"): ",
            false);
        if (thisLine != null)
          martDriver = thisLine;
*/
    
        
        
        //Readline.clearHistory();
//        if (lastDBSettings[PORTITER] != null)
//          Readline.addToHistory(lastDBSettings[PORTITER]);

        //thisLine =  Readline.readline( "\nPort (default: " + DetailedDataSource.DEFAULTPORT +"): ", false);
        thisLine =  myreader.readLine( "\nPort (default: " + DetailedDataSource.DEFAULTPORT +"): ");
        if (thisLine != null)
          martPort = thisLine;

//        Readline.clearHistory();
//       if (lastDBSettings[USERITER] != null)
//          Readline.addToHistory(lastDBSettings[USERITER]);

//        thisLine = Readline.readline("\nUser: ", false);
        thisLine = myreader.readLine("\nUser: ");
        if (thisLine != null)
          martUser = thisLine;

//        Readline.clearHistory();
//        if (lastDBSettings[PASSITER] != null)
//          Readline.addToHistory(lastDBSettings[PASSITER]);

//        thisLine = Readline.readline("\nPassword: ", false);
        thisLine = myreader.readLine("\nPassword: ");
        if (thisLine != null)
          martPass = thisLine;

//        Readline.clearHistory();
//        if (lastDBSettings[DBNAMEITER] != null)
//         Readline.addToHistory(lastDBSettings[DBNAMEITER]);

//        thisLine = Readline.readline("\nDatabase: ", false);
        thisLine = myreader.readLine("\nDatabase: ");
        if (thisLine != null)
          martDatabase = thisLine;

        
//        thisLine = Readline.readline("\nSchema: ", false);
        thisLine = myreader.readLine("\nSchema: ");
        if (thisLine != null)
          martSchema = thisLine;
        
        
//        Readline.clearHistory();
//        if (lastDBSettings[SOURCEKEYITER] != null)
//          Readline.addToHistory(lastDBSettings[SOURCEKEYITER]);

//        thisLine = Readline.readline("\nConnection name (correspond to registry's entry 'name'. NO SPACES plz):  ", false);
        thisLine = myreader.readLine("\nConnection name (correspond to registry's entry 'name'. NO SPACES plz):  ");        
        if (thisLine != null)
          sourceKey = msl.canonicalizeMartName(thisLine); // CHANGED, replace spaces with '_' to prevent having a name with sapces

      } catch (Exception e) {
        throw new InvalidQueryException("Problem reading input for mart connection settings: " + e.getMessage());
      } 
      finally {
        //reset the history
//        Readline.clearHistory();
//        for (int i = 0, n = oldHistory.size(); i < n; i++) {
//         String hist = (String) oldHistory.get(i);
//          Readline.addToHistory(hist);
//        }
      }
    }

    
    
    if (martDatabaseType == null)
      martDatabaseType = DetailedDataSource.DEFAULTDATABASETYPE;

    if (martDriver == null)
      martDriver = DetailedDataSource.DEFAULTDRIVER;

    if (martDatabaseType.equals(DetailedDataSource.DEFAULTDATABASETYPE) && martPort == null)
      martPort = DetailedDataSource.DEFAULTPORT;

    if (sourceKey == null)
      sourceKey = DetailedDataSource.defaultName(martHost, martPort, martDatabase, martSchema,martUser);

    
    //  switched off interactive driver
    martDriver=DetailedDataSource.getJDBCDriverClassNameFor(martDatabaseType);
    
    setLastDatabaseSettings(
      martDatabaseType,
      martHost,
      martPort,
      martDatabase,
      martUser,
      martPass,
      martDriver,
      sourceKey);

    msl.addMart(martDatabaseType, martHost, martPort, martDatabase, martSchema,martUser, martPass, martDriver, sourceKey);
  }

  private void setLastDatabaseSettings(
    String martDatabaseType,
    String martHost,
    String martPort,
    String martDatabase,
    String martUser,
    String martPass,
    String martDriver,
    String sourceKey) {
    lastDBSettings[HOSTITER] = martHost;
    lastDBSettings[DBTYPEITER] = martDatabaseType;
    lastDBSettings[DRIVERITER] = martDriver;
    lastDBSettings[PORTITER] = martPort;
    lastDBSettings[USERITER] = martUser;
    lastDBSettings[PASSITER] = martPass;
    lastDBSettings[DBNAMEITER] = martDatabase;
    lastDBSettings[SOURCEKEYITER] = sourceKey;
  }

  private void removeRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); // skip remove

    if (toks.hasMoreTokens()) {
      String removereq = toks.nextToken();
      if (removereq.equalsIgnoreCase(MARTREQ))
        msl.removeMart(toks);
      else if (removereq.equalsIgnoreCase(DATASETSREQ))
        msl.removeDatasets(toks);
      else if (removereq.equalsIgnoreCase(DATASETREQ))
        msl.removeDataset(toks);
      else if (removereq.equalsIgnoreCase(DATASETCONFIGREQ))
        msl.removeDatasetConfig(toks);
      else if (removereq.equalsIgnoreCase(PROCREQ))
        msl.removeProcedure(toks);
      else
        throw new InvalidQueryException("Invalid remove request recieved " + command + "\n" + Help(REMOVEC) + "\n");
    } else
      throw new InvalidQueryException("Invalid remove request recieved " + command + "\n" + Help(REMOVEC) + "\n");

    updateCompleter();
  }

  private void updateCompleter() {
    if (completionOn) {
      try {
        List martNames = new ArrayList();
        DSConfigAdaptor[] adaptorNames = msl.adaptorManager.getLeafAdaptors();
        for (int i = 0, n = adaptorNames.length; i < n; i++) {
          DSConfigAdaptor adaptor = adaptorNames[i];
          if (adaptor.getNumDatasetConfigs(true) > 0)
            if (!(adaptor instanceof URLDSConfigAdaptor))
              martNames.add( msl.canonicalizeMartName( adaptor.getName() ) );
        }
        mcl.setMartNames(martNames);
        
        mcl.setAdaptorLocations(Arrays.asList(msl.adaptorManager.getAdaptorNames()));
        mcl.setProcedureNames(msl.getStoredMQLCommandKeys());

        List datasetInames = new ArrayList();
        if (msl.envMart != null) {
          //get all datasets relative to envMart
          DSConfigAdaptor adaptor = msl.adaptorManager.getAdaptorByName( msl.envMart.getName() );

          String[] datasets = adaptor.getDatasetNames(false);
          for (int i = 0, n = datasets.length; i < n; i++) {
            datasetInames.add(datasets[i]);
          }
        } else {
          //dump absolute path names for all Datasets (not configs)
          String[] adaptors = msl.adaptorManager.getAdaptorNames();
          for (int i = 0, n = adaptors.length; i < n; i++) {
            String adaptor = adaptors[i];

            String[] datasets = msl.adaptorManager.getAdaptorByName(adaptor).getDatasetNames(false);
            for (int j = 0, m = datasets.length; j < m; j++) {
              String dataset = datasets[j];
              datasetInames.add(msl.canonicalizeMartName( adaptor ) + "." + dataset);
            }
          }
        }
        mcl.setDatasetConfigInternalNames(datasetInames);
      } catch (ConfigurationException e) {
        if (mainLogger.isLoggable(Level.INFO))
          mainLogger.info("Caught ConfigurationException updating the completion system\n");
      }
    }
  }

  private void updateRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); // skip update

    if (toks.hasMoreTokens()) {
      String updatereq = toks.nextToken();
      if (updatereq.equalsIgnoreCase(DATASETSREQ))
        msl.updateDatasets(toks);
      else if (updatereq.equalsIgnoreCase(DATASETREQ))
        msl.updateDataset(toks);
      else
        throw new InvalidQueryException("Invalid update request recieved " + command + "\n" + Help(UPDATEC) + "\n");
    } else
      throw new InvalidQueryException("Invalid update request recieved " + command + "\n" + Help(UPDATEC) + "\n");

    updateCompleter();
  }
    
  private void unsetRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); // skip unset

    if (toks.hasMoreTokens()) {
      String request = toks.nextToken();

      if (request.equalsIgnoreCase(PROMPTREQ))
        unsetPrompt();
      else if (request.equalsIgnoreCase(MARTREQ))
        try {
          msl.setEnvMart(null);
        } catch (InvalidQueryException e) {
          throw new InvalidQueryException(e.getMessage() + "\n" + Help(UNSETC) + "\n");
        } else if (request.equalsIgnoreCase(OUTPUTREQ))
        unsetOutputSettings(toks);
      else if (request.equalsIgnoreCase(VERBOSEREQ))
        unsetVerbose();
      else if (request.equalsIgnoreCase(DATASETREQ)) {
        try {
          msl.setEnvDataset(null);
        } catch (InvalidQueryException e) {
          throw new InvalidQueryException(e.getMessage() + "\n" + Help(UNSETC) + "\n");
        }
    } else if (request.equalsIgnoreCase(ADVANCEDREQ))
      msl.setAdvancedFeatures(false); 
    } else
      throw new InvalidQueryException("Recieved invalid unset command " + command + "\n" + Help(SETC));

    updateCompleter();
  }

  private void unsetPrompt() throws InvalidQueryException {
    userPrompt = null;
  }

  private void unsetVerbose() throws InvalidQueryException {
    if (loggingConfURL == null) {
      verbose = false;

      LoggingUtils.setVerbose(verbose);

      if (mainLogger.isLoggable(Level.INFO))
        mainLogger.info("Logging now off\n");
    } else
      throw new InvalidQueryException("Cannot change logging properties when a logging configuration URL is supplied\n");
  }

  private void unsetOutputSettings(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      try {
        while (toks.hasMoreTokens()) {
          String key = toks.nextToken(",").trim();

          if (key.equals(FILE)) {
            sessionOutput = DEFOUTPUT;
            appendToFile = false;
            sessionOutputFileName = null;
          } else if (key.equals(FORMAT))
            sessionOutputFormat = DEFOUTPUTFORMAT;
          else if (key.equals(SEPARATOR))
            sessionOutputSeparator = DEFOUTPUTSEPARATOR;
          else
            throw new InvalidQueryException("Recieved invalid unset Output request " + key + "\n" + Help(UNSETC));
        }
      } catch (Exception e) {
        throw new InvalidQueryException("Could not unset output settings: " + e.getMessage() + "\n", e);
      }
    } else {
      sessionOutput = DEFOUTPUT;
      sessionOutputFileName = null;
      sessionOutputFormat = DEFOUTPUTFORMAT;
      appendToFile = false;
      sessionOutputSeparator = DEFOUTPUTSEPARATOR;
    }
  }

  private void setRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); // skip set

    if (toks.hasMoreTokens()) {
      String request = toks.nextToken();

      if (request.equalsIgnoreCase(PROMPTREQ))
        setPrompt(toks);
      else if (request.equalsIgnoreCase(MARTREQ)) {
        if (!toks.hasMoreTokens())
          throw new InvalidQueryException("Invalid set Mart command\n" + Help(SETC) + "\n");

        try {
          msl.setEnvMart(toks.nextToken());
        } catch (InvalidQueryException e) {
          throw new InvalidQueryException(e.getMessage() + "\n" + Help(SETC) + "\n");
        }
      } else if (request.equalsIgnoreCase(OUTPUTREQ))
        setOutputSettings(toks);
      else if (request.equalsIgnoreCase(VERBOSEREQ))
        setVerbose(toks);
      else if (request.equalsIgnoreCase(DATASETREQ)) {
        if (!toks.hasMoreTokens())
          throw new InvalidQueryException("Invalid set dataset command\n" + Help(SETC) + "\n");
        try {
          msl.setEnvDataset(toks.nextToken());
        } catch (InvalidQueryException e) {
          throw new InvalidQueryException(e.getMessage() + "\n" + Help(SETC) + "\n");
        }
      } else if (request.equalsIgnoreCase(ADVANCEDREQ))
        msl.setAdvancedFeatures(true);
    } else
      throw new InvalidQueryException("Recieved invalid set command " + command + "\n" + Help(SETC));

    updateCompleter();
  }

  private void setPrompt(StringTokenizer toks) throws InvalidQueryException {
    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid set Prompt Command Recieved\n" + Help(SETC));

    String prompt = toks.nextToken();

    if (prompt.equals("-"))
      userPrompt = null;
    else
      userPrompt = prompt + " >";
  }

  private void setVerbose(StringTokenizer toks) throws InvalidQueryException {
    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid set Verbose Command Recieved\n" + Help(SETC));

    String command = toks.nextToken();
    if (loggingConfURL == null) {
      if (command.equals("on"))
        verbose = true;
      else if (command.equals("off"))
        verbose = false;
      else
        throw new InvalidQueryException("Invalid set Verbose command recieved: \n" + Help(SETC));

      LoggingUtils.setVerbose(verbose);

      if (mainLogger.isLoggable(Level.INFO))
        mainLogger.info("Logging now " + command + "\n");
    } else
      throw new InvalidQueryException("Cannot change logging properties when a logging configuration URL is supplied\n");
  }

  private void setOutputSettings(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      try {
        String fSettings = toks.nextToken();

        while (toks.hasMoreTokens())
          fSettings += " " + toks.nextToken();

        fSettings = fSettings.replaceAll("\\s+", "");

        //pattern to find all occurances of x='y' in a string
        Pattern pat = Pattern.compile("(\\w+\\=\\'[^\\']+\\')");
        Matcher mat = pat.matcher(fSettings);
        boolean matchFound = false;

        while (mat.find()) {
          matchFound = true;
          String setReq = mat.group();

          String[] setkv = setReq.split("\\=");

          String key = setkv[0];
          String value = setkv[1];

          value = value.substring(value.indexOf("'") + 1, value.lastIndexOf("'"));
          // strip off leading and trailing quotes

          if (key.equals(FILE)) {
            if (value.equals("-")) {
              sessionOutputFileName = null;
              appendToFile = false;
              sessionOutput = DEFOUTPUT;
            } else {
              if (value.startsWith(">>")) {
                appendToFile = true;
                value = value.substring(2);
              } else
                appendToFile = false;
              sessionOutputFileName = value;
            }
          } else if (key.equals(FORMAT))
            sessionOutputFormat = value;
          else if (key.equals(SEPARATOR))
            sessionOutputSeparator = value;
          else
            throw new InvalidQueryException("Recieved invalid set Output request " + fSettings + "\n" + Help(SETC));
        }

        if (!matchFound)
          throw new InvalidQueryException("Invalid set Output request " + fSettings + "\n" + Help(SETC));

      } catch (Exception e) {
        throw new InvalidQueryException("Could not set output settings: " + e.getMessage() + "\n", e);
      }
    } else {
      //interactive
      if (mainBatchMode)
        throw new InvalidQueryException("Recieved invalid add Mart command.\n" + Help(ADDC) + "\n");

      String thisLine = null;

      try {
        String out = (sessionOutputFormat != null) ? sessionOutputFormat : DEFOUTPUTFORMAT;
        thisLine =
//          Readline.readline(
            myreader.readLine(
        	"\nPlease enter the format of the output (either 'tabulated' or 'fasta', enter '-' to use "
              + DEFOUTPUTFORMAT
              + ", hit enter to leave as "
              + out
              + "): ");              
//              + "): ",
//            false);
        if (thisLine != null) {
          if (thisLine.equals("-"))
            sessionOutputFormat = DEFOUTPUTFORMAT;
          else
            sessionOutputFormat = thisLine;
        }

        out = (sessionOutputFileName != null) ? sessionOutputFileName : DEFOUTPUTFILE;
        out = (appendToFile) ? ">>" + out : out;
        thisLine =
//          Readline.readline(
            myreader.readLine(        	
            "\nPlease enter the File to output all MQL commands (use '-' for "
              + DEFOUTPUTFILE
              + ", hit enter to leave as "
              + out
              + ", prepend path with '>>' to append to an existing file): ");
//          false);
        if (thisLine != null) {
          if (thisLine.equals("-")) {
            sessionOutput = DEFOUTPUT;
            appendToFile = false;
            sessionOutputFileName = null;
          } else {
            if (thisLine.startsWith(">>")) {
              appendToFile = true;
              thisLine = thisLine.substring(2);
            } else
              appendToFile = false;
            sessionOutputFileName = thisLine;
          }
        }

        out = (sessionOutputSeparator != null) ? sessionOutputSeparator : DEFOUTPUTSEPARATOR;

        thisLine =
//        Readline.readline(
            myreader.readLine(
            "\nPlease enter the record separator to use (use '-' for "
              + DEFOUTPUTSEPARATOR
              + ", hit enter to leave as "
              + out
              + "): ");              
//              + "): ",
//            false);

        if (thisLine != null) {
          if (thisLine.equals("-"))
            sessionOutputSeparator = DEFOUTPUTSEPARATOR;
          else
            sessionOutputSeparator = thisLine;
        }

      } catch (Exception e) {
        throw new InvalidQueryException("Problem reading input for mart connection settings: " + e.getMessage(), e);
      }
    }

  }

  private void envRequest(String command) throws InvalidQueryException {
    System.out.println();

    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); //skip environment

    if (!toks.hasMoreTokens()) {
      showAllEnvironment();
    } else {
      String req = toks.nextToken();
      if (req.equalsIgnoreCase(MARTREQ)) {
        System.out.println(msl.showEnvMart());
      } else if (req.equalsIgnoreCase(DATASETREQ)) {
        System.out.println(msl.showEnvDataset());
      } else if (req.equalsIgnoreCase(DATASETCONFIGREQ)) {
        System.out.println(msl.showEnvDataSetConfig());
      } else if (req.equalsIgnoreCase(OUTPUTREQ)) {
        try {
          showEnvOutput(toks);
        } catch (InvalidQueryException e) {
          throw new InvalidQueryException(e.getMessage() + "\n" + Help(ENVC) + "\n");
        }
      } else
        throw new InvalidQueryException("Recieved invalid environment request " + command + "\n" + Help(ENVC));

      System.out.println();
    }
  }

  private void showAllEnvironment() {
    System.out.println(msl.showEnvMart());
    System.out.println(msl.showEnvDataset());
    System.out.println(msl.showEnvDataSetConfig());
    showAllOutputSettings();
  }

  private void showEnvOutput(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String subreq = toks.nextToken();
      if (subreq.equalsIgnoreCase(FORMAT)) {
        String out = (sessionOutputFormat != null) ? sessionOutputFormat : DEFOUTPUTFORMAT;
        System.out.println(" " + FORMAT + " = " + out);
      } else if (subreq.equalsIgnoreCase(FILE)) {
        String out = (sessionOutputFileName != null) ? sessionOutputFileName : DEFOUTPUTFILE;
        System.out.println(" " + FILE + " = " + out);
      } else if (subreq.equalsIgnoreCase(SEPARATOR)) {
        String out = (sessionOutputSeparator != null) ? sessionOutputSeparator : DEFOUTPUTSEPARATOR;
        System.out.println(" " + SEPARATOR + " = " + out);
      } else
        throw new InvalidQueryException("Recieved invalid Environment Output commmand " + subreq + "\n" + Help(ENVC));
    } else
      showAllOutputSettings();

    System.out.println();
  }

  private void showAllOutputSettings() {
    String thisFormat = (sessionOutputFormat != null) ? sessionOutputFormat : DEFOUTPUTFORMAT;
    String thisFile = (sessionOutputFileName != null) ? sessionOutputFileName : DEFOUTPUTFILE;
    String thisSeparator = (sessionOutputSeparator != null) ? sessionOutputSeparator : DEFOUTPUTSEPARATOR;

    System.out.println(
      " Output Format: "
        + FORMAT
        + " = "
        + thisFormat
        + ", "
        + SEPARATOR
        + " = "
        + "'"
        + thisSeparator
        + "'"
        + ", "
        + FILE
        + " = "
        + thisFile);

    System.out.println();
  }

  private void useRequest(String command) throws InvalidQueryException {
    
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); //skip use

    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid Use Command Recieved: " + command + "\n");
      
    msl.setEnvDataset(toks.nextToken());

    updateCompleter();
  }

  private void WriteHistory(String command) throws InvalidQueryException {
    try {
      StringTokenizer com = new StringTokenizer(command, " ");
      int tokCount = com.countTokens();
      com.nextToken(); // skip commmand start

      String req = null;

      String outPutFileName = null;
      File outPutFile = null;

      if (tokCount < 2)
        throw new InvalidQueryException("WriteHistory command must be provided a valid URL: " + command + "\n");
      else if (tokCount == 2) {
        //file
        outPutFileName = com.nextToken();
        outPutFile = new File(outPutFileName);
      } else if (tokCount == 3) {
        req = com.nextToken();
        outPutFileName = com.nextToken();
        outPutFile = new File(outPutFileName);
      } else
        throw new InvalidQueryException("Recieved invalid WriteHistory request " + command + "\n");

      WriteHistoryLinesToFile(req, outPutFile);

    } catch (Exception e) {
      throw new InvalidQueryException("Could not write history " + e.getMessage());
    }
  }

  private void WriteHistoryLinesToFile(String req, File outPutFile) throws InvalidQueryException {
    String[] lines = GetHistoryLines(req);
    // will throw an exception if GetHistoryLines requirements are not satisfied

    try {
      OutputStreamWriter hisout = new OutputStreamWriter(new FileOutputStream(outPutFile));
      for (int i = 0, n = lines.length; i < n; i++) {
        String thisline = lines[i];
        if (!thisline.startsWith(SAVETOSCRIPTC))
          hisout.write(thisline + "\n");
      }
      hisout.close();
    } catch (Exception e) {
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private void LoadScript(String command) throws InvalidQueryException {
    StringTokenizer com = new StringTokenizer(command, " ");
    if (!(com.countTokens() > 1))
      throw new InvalidQueryException("Recieved invalid LoadScript command, must supply a URL\n");

    com.nextToken(); // skip command start

    String scriptFile = null;
    try {
      scriptFile = com.nextToken();
      LoadScriptFromFile(scriptFile);
    } catch (Exception e) {
      throw new InvalidQueryException("Could not load script: " + scriptFile + " " + e.getMessage());
    }
  }

  private void LoadScriptFromFile(String scriptFile) throws InvalidQueryException {
    if (!readlineLoaded)
      throw new InvalidQueryException("Sorry, histrory functions are not available on your terminal.\n");
    if (!historyOn)
      throw new InvalidQueryException("Sorry, histrory is not activated.\n");
    
    File fobj = new File(scriptFile);
	try{
		hisObj.setHistoryFile(fobj);
	}
	catch (IOException e) {
		System.out.println("LoadScriptFromFile: Unable to set history file");
	}
    
  }

  private void executeRequest(String command) throws InvalidQueryException {
    StringTokenizer toks = new StringTokenizer(command, " ");
    toks.nextToken(); //skip execute

    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid execute command recieved " + command + "\n" + Help(EXECC));

    String request = toks.nextToken();
    if (request.equalsIgnoreCase(HISTORYC))
      executeHistory(toks);
    else if (request.equalsIgnoreCase(PROCREQ))
      executeProcedure(toks);
    else if (request.equalsIgnoreCase(SCRIPTREQ))
      ExecuteScript(toks);
    else
      throw new InvalidQueryException("Invalid execute command recieved " + command + "\n" + Help(EXECC));
  }

  private void executeHistory(StringTokenizer toks) throws InvalidQueryException {
    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid execute history command recieved\n" + Help(EXECC));

    String req = null;
    try {
      req = toks.nextToken();

      String[] lines = GetHistoryLines(req);
      // will throw an exception if GetHistoryLines requirements are not satisfied
      for (int i = 0, n = lines.length; i < n; i++) {
        String thisline = lines[i];

        if (historyOn)
        {
          //Readline.addToHistory(thisline);
        	this.addToHistory(thisline);
        }
        while (thisline != null)
          thisline = parseForCommands(thisline);
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Could not execute history " + req + " " + e.getMessage(), e);
    }
  }
  

  private void executeProcedure(StringTokenizer toks) throws InvalidQueryException {
    if (!toks.hasMoreTokens())
      throw new InvalidQueryException("Invalid execute procedure command recieved\n" + Help(EXECC));

    String storedCommandName = toks.nextToken();
    String nestedQuery = getMQLForStoredProcedure(storedCommandName);
    try {
      executeCommand(nestedQuery);
    } catch (Exception e) {
      throw new InvalidQueryException("Recieved Exception executing Stored Procedure " + e.getMessage() + "\n", e);
    }
  }

  private String getMQLForStoredProcedure(String storedCommandName) throws InvalidQueryException {
    String bindValues = null;
    if (storedCommandName.indexOf(MartShellLib.LISTSTARTCHR) > 0) {
      bindValues =
        storedCommandName.substring(
          storedCommandName.indexOf(MartShellLib.LISTSTARTCHR) + 1,
          storedCommandName.indexOf(MartShellLib.LISTENDCHR));
      storedCommandName = storedCommandName.substring(0, storedCommandName.indexOf(MartShellLib.LISTSTARTCHR));
    }

    String nestedQuery = msl.describeStoredMQLCommand(storedCommandName);

    if (nestedQuery != null) {
      if ((bindValues != null) && (bindValues.length() > 0)) {
        List bindVariables = new ArrayList();
        StringTokenizer vtokens = new StringTokenizer(bindValues, ",");
        while (vtokens.hasMoreTokens())
          bindVariables.add(vtokens.nextToken().trim());

        Pattern bindp = Pattern.compile("\\?");
        Matcher bindm = bindp.matcher(nestedQuery);

        StringBuffer qbuf = new StringBuffer();
        int bindIter = 0;
        while (bindm.find()) {
          bindm.appendReplacement(qbuf, (String) bindVariables.get(bindIter));
          bindIter++;
        }
        bindm.appendTail(qbuf);
        nestedQuery = qbuf.toString();
      }
      return nestedQuery;
    } else
      throw new InvalidQueryException("Procedure for " + storedCommandName + " has not been defined\n");
  }

  private void ExecuteScript(StringTokenizer toks) throws InvalidQueryException {
    if (!(toks.hasMoreTokens()))
      throw new InvalidQueryException("Recieved invalid execute Script command, must supply a URL or path\n");

    String source = toks.nextToken();

    try {
      InputStream inStream = InputSourceUtil.getStreamForString(source);
      ExecScriptFromStream(inStream);
      inStream.close();
    } catch (Exception e) {
      throw new InvalidQueryException("Could not execute script: " + source + " " + e.getMessage(), e);
    }
  }

  private void ExecScriptFromStream(InputStream input) throws InvalidQueryException {
      String line = null;
      try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input));

      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.startsWith("#")) {
          if (historyOn)
          {
            //Readline.addToHistory(line);
        	  this.addToHistory(line);
          }
          parse(line);
        }
      }
    } catch (Exception e) {
      throw new InvalidQueryException(e.getMessage() + "\n last Line: " + line + "\n", e);
    }
  }

  private void History(String command) throws InvalidQueryException {
    try {
      StringTokenizer com = new StringTokenizer(command, " ");
      int start = 1;

      String req = null;

      if (com.countTokens() > 1) {
        com.nextToken(); // skip commmand start
        req = com.nextToken();

        int compos = req.indexOf(",");
        if (compos >= 0) {
          if (compos > 0) {
            //n, or n,y
            try {
              start = Integer.parseInt(req.substring(0, compos));
            } catch (NumberFormatException nfe) {
              throw new InvalidQueryException(nfe.getMessage(), nfe);
            }
          }
        } else //n
          try {
            start = Integer.parseInt(req);
          } catch (NumberFormatException nfe) {
            throw new InvalidQueryException(nfe.getMessage(), nfe);
          }
      }

      String[] lines = GetHistoryLines(req);
      // will throw an exception if GetHistoryLines requirements are not satisfied
      for (int i = 0, n = lines.length; i < n; i++) {
        System.out.print(start + " " + lines[i] + "\n");
        start++;
      }
    } catch (Exception e) {
      throw new InvalidQueryException("Could not show history " + e.getMessage(), e);
    }
  }

  private String[] GetHistoryLines(String req) throws InvalidQueryException {
    if (!readlineLoaded)
      throw new InvalidQueryException("Sorry, histrory functions are not available on your terminal.\n");
    if (!historyOn)
      throw new InvalidQueryException("Sorry, histrory is not activated.\n");

    List lines = new ArrayList();
    
    if (req == null){
      //Readline.getHistory(lines);
    	lines = this.getHistory();
    }
    else {
      int start = 0;
      int end = 0;

      if (req.indexOf(",") > -1) {
        StringTokenizer pos = new StringTokenizer(req, ",", true);

        if (pos.countTokens() > 2) {
          //n,y
          try {
            start = Integer.parseInt(pos.nextToken()) - 1;
            pos.nextToken(); // skip ,
            end = Integer.parseInt(pos.nextToken());
          } catch (NumberFormatException nfe) {
            throw new InvalidQueryException(nfe.getMessage(), nfe);
          }
        } else {
          //either n, or ,y
          try {
            String tmp = pos.nextToken();
            if (tmp.equals(",")) {
              start = 0;
              end = Integer.parseInt(pos.nextToken());
            } else {
              start = Integer.parseInt(tmp) - 1;
              //end = Readline.getHistorySize();
              end = this.getHistorySize();
            }
          } catch (NumberFormatException nfe) {
            throw new InvalidQueryException(nfe.getMessage(), nfe);
          }
        }

        for (int i = start; i < end; i++)
        {
        	//lines.add(Readline.getHistoryLine(i));
        	lines.add(this.getHistoryLine(i));
        }
      } else {
        try {
          start = Integer.parseInt(req) - 1;
        } catch (NumberFormatException nfe) {
          throw new InvalidQueryException(nfe.getMessage(), nfe);
        }
        //lines.add(Readline.getHistoryLine(start));
        lines.add(this.getHistoryLine(start));
      }
    }
    String[] ret = new String[lines.size()];
    lines.toArray(ret);
    return ret;
  }
  
  private void addToHistory(String line) {
	  //TODO: ADD a single line to history of the current instance of console reader.
	  hisObj.addToHistory(line);
  }
  private List getHistory() {
	  //TODO: 
	  return hisObj.getHistoryList();
  }
  private int getHistorySize() {
	  //TODO: 
	  return hisObj.size();	  
  }
  private Object getHistoryLine(int lno) {
	  //TODO: 
	  List lines =  new LinkedList(); 
	  lines = hisObj.getHistoryList();
	  return lines.get(lno);
	  
  }
  /**
   * Takes a String MQL line, and parses it for commands to execute.  If any
   * complete commands are found, they are executed.
   * If any incomplete commands are encountered (either after a complete command, separated by semicolon, 
   * or in the middle of a single command), the line is cached for later addition by subsequent
   * calls to parse. If the current line completes a command built up over successive previous 
   * calls to parse, this command is executed.
   * @param line -- String line to parse
   * @throws SequenceException
   * @throws FormatException
   * @throws IOException
   * @throws SQLException
   * @throws InvalidQueryException
   * @throws ConfigurationException
   */
  public void parse(String line)
    throws SequenceException, FormatException, IOException, SQLException, InvalidQueryException, ConfigurationException {
    if (line.indexOf(LINEEND) >= 0) {
      String currentCommand = conline.append(" ").append(line).toString().trim();
      conline = new StringBuffer(); // may be reinitialized with residual

      String residual = parseForCommands(currentCommand);
      if (residual != null) {
        continueQuery = true;
        conline = new StringBuffer(residual);

        if (completionOn)
          mcl.setModeForLine(residual);
      } else
        continueQuery = false;
    } else {
      conline.append(" ").append(line);
      continueQuery = true;

      //MartCompleter Mode
      if (completionOn)
        mcl.setModeForLine(line);
    }
  }

  private String parseForCommands(String line)
    throws SequenceException, FormatException, IOException, SQLException, InvalidQueryException, ConfigurationException {
    StringBuffer residual = new StringBuffer();

    StringTokenizer commandtokens = new StringTokenizer(line, LINEEND, true);

    while (commandtokens.hasMoreTokens()) {
      String thisCommand = commandtokens.nextToken().trim();
      if (thisCommand.equals(LINEEND)) {
        if (residual.length() > 1) {
          executeCommand(residual.toString());
          residual = new StringBuffer();
        }
      } else
        residual = new StringBuffer(thisCommand);
    }

    if (residual.length() > 0)
      return residual.toString();
    else
      return null;
  }

  private DatasetRequest validDatasetRequest(String command) {
    DatasetRequest ret = null;

    try {
      ret = new DatasetRequest(command, msl); //it will throw an exception if this is not a valid request
    } catch (InvalidQueryException e) {
      ret = null;
    }

    return ret;
  }

  /**
   * Takes a complete MQL command, and executes it.
   * @param command -- MQL to execute
   * @throws SequenceException
   * @throws FormatException
   * @throws IOException
   * @throws SQLException
   * @throws InvalidQueryException
   * @throws ConfigurationException
   */
  public void executeCommand(String command)
    throws SequenceException, FormatException, IOException, SQLException, InvalidQueryException, ConfigurationException {
    int cLen = command.length();

    command = command.replaceAll("\\s;$", ";");
    // removes any whitespace before the ; character

    if (cLen == 0)
      return;
    else if (command.startsWith(USEC))
      useRequest(normalizeCommand(command));
    else if (command.startsWith(ENVC))
      envRequest(normalizeCommand(command));
    else if (command.startsWith(HELPC))
      System.out.print(Help(normalizeCommand(command)));
    else if (command.startsWith(DESCC))
      describeRequest(normalizeCommand(command));
    else if (command.startsWith(LISTC))
      listRequest(normalizeCommand(command));
    else if (command.startsWith(ADDC))
      addRequest(normalizeCommand(command));
    else if (command.startsWith(REMOVEC))
      removeRequest(normalizeCommand(command));
    else if (command.startsWith(SETC))
      setRequest(normalizeCommand(command));
    else if (command.startsWith(UNSETC))
      unsetRequest(normalizeCommand(command));
    else if (command.startsWith(UPDATEC))
      updateRequest(normalizeCommand(command));
    else if (command.startsWith(HISTORYC))
      History(normalizeCommand(command));
    else if (command.startsWith(EXECC))
      executeRequest(normalizeCommand(command));
    else if (command.startsWith(LOADSCRPTC))
      LoadScript(normalizeCommand(command));
    else if (command.startsWith(SAVETOSCRIPTC))
      WriteHistory(normalizeCommand(command));
    else if (normalizeCommand(command).equals(EXITC) || normalizeCommand(command).equals(QUITC))
      ExitShell();
    else if (
      command.startsWith(MartShellLib.GETQSTART)
        || command.startsWith(MartShellLib.USINGQSTART)
        || command.startsWith(COUNTFOCUSC)
    //    || command.startsWith(COUNTROWSC)
    		) 
    
    {
      //is it a store command
      Matcher storeMatcher = MartShellLib.STOREPAT.matcher(command);
      if (storeMatcher.matches()) {

        String storedCommand = storeMatcher.group(1);
        String key = storeMatcher.group(4);

        if (key.indexOf(LINEEND) > 0)
          key = key.substring(0, key.indexOf(LINEEND));

        msl.addStoredMQLCommand(key, storedCommand.toString());

        if (completionOn)
          mcl.setProcedureNames(msl.getStoredMQLCommandKeys());
      } else {
        boolean countFocus = false;

        Query query = null;
        if (command.startsWith(COUNTFOCUSC)) {
          command = command.substring(COUNTFOCUSC.length()).trim();

          if (command.split("\\s+").length == 1) {
            //must be a stored procedure, or a dataset request
            DatasetRequest dsrq = validDatasetRequest(normalizeCommand(command));
            if (dsrq != null) {
              query = new Query();
              query.setDataSource(msl.adaptorManager.getAdaptorByName( dsrq.mart ).getDataSource());

              DatasetConfig thisDatasetConfig =
                msl.adaptorManager.getDatasetConfigByDatasetInternalName(dsrq.dataset, dsrq.datasetconfig);
                
              query.setDataset(thisDatasetConfig.getDataset());
              query.setMainTables(thisDatasetConfig.getStarBases());
              query.setPrimaryKeys(thisDatasetConfig.getPrimaryKeys());

            } else
              command = getMQLForStoredProcedure(normalizeCommand(command));
          }

          countFocus = true;
        }

        //will only be not null if it is a count_x_from dataset_request command
        if (query == null)
          query = msl.MQLtoQuery(command);

        FormatSpec fspec = null;

        if (sessionOutputFormat != null) {
          if (sessionOutputFormat.equals("fasta"))
            fspec = FormatSpec.FASTAFORMAT;
          else {
            fspec = new FormatSpec(FormatSpec.TABULATED);

            if (sessionOutputSeparator != null)
              fspec.setSeparator(sessionOutputSeparator);
            else
              fspec.setSeparator(DEFOUTPUTSEPARATOR);
          }

        } else
          fspec = FormatSpec.TABSEPARATEDFORMAT;

        //no hardLimit for -e/-E
        int hardLimit = 0;
        
        if (interactiveMode) {
          if (query.getLimit() > 0)
            hardLimit = Math.min(INTERACTIVE_MAX_ROWS, query.getLimit()); //user may have supplied a limit
          else
            hardLimit = INTERACTIVE_MAX_ROWS;            
        }
          
          
        if (sessionOutputFileName != null) {
          hardLimit = Math.max(0, query.getLimit()); //no hardLimit for file output, even in interactive mode, unless user specifies
          sessionOutput = new FileOutputStream(sessionOutputFileName, appendToFile);
        }

        Engine engine = new Engine();

        if (countFocus)
          engine.countFocus(sessionOutput, query);
        else
          engine.execute(query, fspec, sessionOutput, hardLimit);

        if (sessionOutputFileName != null)
          sessionOutput.close();
      }
    } else {
      throw new InvalidQueryException("\nInvalid Command: please try again " + command + "\n");
    }
  }

  /**
   * Special Method for MartShellTest, allows it to set the
   * session OutputStream to an OutputStream that it can
   * manipulate.  If out is null, defaults to System.out
   */
  public void setTestOutput(OutputStream out) {
    if (out != null)
      sessionOutput = out;
    else
      sessionOutput = DEFOUTPUT;
  }

  // MartShell instance variables
  private boolean interactiveMode = false;
  private final int INTERACTIVE_MAX_ROWS = 1000;
  private MartShellLib msl = null;
  private boolean verbose = false;
  private URL loggingConfURL = null;

  private final String history_file = System.getProperty("user.home") + "/.martshell_history";

  private MartCompleter mcl;
  // will hold the MartCompleter, if Readline is loaded and completion turned on
  private boolean helpLoaded = false;
  // first time Help function is called, loads the help properties file and sets this to true
  private boolean historyOn = false; // commandline history, default to off
  private boolean completionOn = false; // commannd completion, default to off
  private boolean readlineLoaded = false;
  // true only if functional Readline library was loaded, false if PureJava
  private String userPrompt = null;
  private final String DEFAULTPROMPT = "MartShell";

  //these are set using the set Output command.
  private final OutputStream DEFOUTPUT = System.out;
  private OutputStream sessionOutput = DEFOUTPUT; // defaults to System.out, can be chaged
  private String sessionOutputFileName = null;
  private String sessionOutputFormat = null;
  private String sessionOutputSeparator = null;

  //defaults for output settings tabulated, tab separated, STDOUT
  private final String DEFOUTPUTFORMAT = "tabulated";
  private final String DEFOUTPUTSEPARATOR = "\t";
  private final String DEFOUTPUTFILE = "STDOUT";
  private boolean appendToFile = false;

  // this is set using the setOutputSettings command.

  private String batchErrorMessage = null;
  private Properties commandHelp = new Properties();
  private Properties supportHelp = new Properties();

  private final String HELPFILE = "data/help.properties";
  //contains help general to the shell
  private final String DSHELPFILE = "data/dshelp.properties";
  // contains help for domain specific aspects
  private final String HELPSUPPORT = "data/helpSupport.properties";
  private final String DSHELPSUPPORT = "data/dshelpSupport.properties";

  private final Pattern DOMAINSPP = Pattern.compile("DOMAINSPECIFIC:(\\w+)", Pattern.DOTALL);
  private Pattern INSERTP = Pattern.compile("INSERT:(\\w+)", Pattern.DOTALL);

  // available commands
  private final String EXITC = "exit";
  private final String QUITC = "quit";
  private final String HELPC = "help";
  private final String DESCC = "describe";
  private final String USEC = "use";
  private final String SETC = "set";
  private final String UNSETC = "unset";
  private final String ENVC = "environment";
  private final String ADDC = "add";
  private final String REMOVEC = "remove";
  private final String UPDATEC = "update";
  private final String EXECC = "execute";
  private final String LOADSCRPTC = "loadScript";
  private final String SAVETOSCRIPTC = "saveToScript";
  private final String HISTORYC = "history";
  private final String LISTC = "list";
  private final String COUNTFOCUSC = "count_focus_from";
  private final String SCRIPTREQ = "Script";
  private final String MARTREQ = "Mart";
  private final String MARTSREQ = "Marts";
  private final String DATASETREQ = "dataset";
  private final String DATASETSREQ = "datasets";
  private final String DATASETCONFIGSREQ = "datasetconfigs";
  private final String DATASETCONFIGREQ = "datasetconfig";
  private final String FILTERSREQ = "filters";
  private final String FILTERREQ = "filter";
  private final String ATTRIBUTESREQ = "attributes";
  private final String ATTRIBUTEREQ = "attribute";
  private final String PROCREQ = "procedure";
  private final String PROCSREQ = "procedures";
  private final String PROMPTREQ = "prompt";
  private final String OUTPUTREQ = "output";
  private final String VERBOSEREQ = "verbose";
  private final String ADVANCEDREQ = "advancedFeatures";

  //lists to set for completion of add, remove, list, set, update, describe, environment, execute
  private final List addRequests =
    Collections.unmodifiableList(new ArrayList(Arrays.asList(new String[] { MARTREQ, DATASETSREQ, DATASETCONFIGREQ })));

  private final List removeRequests =
    Collections.unmodifiableList(
      new ArrayList(Arrays.asList(new String[] { MARTREQ, DATASETSREQ, DATASETREQ, DATASETCONFIGREQ, PROCREQ })));

  private final List listRequests =
    Collections.unmodifiableList(
      new ArrayList(
        Arrays.asList(new String[] { MARTSREQ, DATASETSREQ, DATASETCONFIGSREQ, FILTERSREQ, ATTRIBUTESREQ, PROCSREQ })));

  private final List setRequests =
    Collections.unmodifiableList(
      new ArrayList(Arrays.asList(new String[] { MARTREQ, DATASETREQ, PROMPTREQ, OUTPUTREQ, VERBOSEREQ, ADVANCEDREQ })));

  private final List updateRequests =
    Collections.unmodifiableList(new ArrayList(Arrays.asList(new String[] { DATASETSREQ, DATASETREQ })));

  private final List describeRequests =
    Collections.unmodifiableList(
      new ArrayList(Arrays.asList(new String[] { MARTREQ, DATASETREQ, FILTERREQ, ATTRIBUTEREQ, PROCREQ })));

  private final List envRequests =
    Collections.unmodifiableList(
      new ArrayList(Arrays.asList(new String[] { MARTREQ, OUTPUTREQ, DATASETREQ, DATASETCONFIGREQ })));

  private final List executeRequests =
    Collections.unmodifiableList(new ArrayList(Arrays.asList(new String[] { PROCREQ, HISTORYC, SCRIPTREQ })));

  protected List availableCommands =
    new ArrayList(
      Arrays.asList(
        new String[] {
          EXECC,
          LOADSCRPTC,
          SAVETOSCRIPTC,
          EXECC,
          EXITC,
          QUITC,
          HELPC,
          SETC,
          UNSETC,
          ENVC,
          ADDC,
          REMOVEC,
          UPDATEC,
          DESCC,
          LISTC,
          USEC,
          COUNTFOCUSC,
        //  COUNTROWSC 
        }));

  // strings used to show/set output format settings
  private final String FILE = "file";
  private final String FORMAT = "format";
  private final String SEPARATOR = "separator";

  // strings used to show/set mart connection settings
  private final String DBHOST = "host";
  private final String DBUSER = "user";
  private final String DBPASSWORD = "password";
  private final String DBPORT = "port";
  private final String INSTANCENAME = "instanceName";
  private final String DATABASETYPE = "databaseType";
  private final String DBDRIVER = "jdbcDriver";

  //startup message variables
  private final String STARTUP = "startUp";
  private final String HISTORYQ = "CommandHistoryHelp";
  private final String COMPLETIONQ = "CommandCompletionHelp";

  private final int MAXLINECOUNT = 60;
  // page prompt describe output line limit
  private final int MAXCHARCOUNT = 80; // line length limit

  private boolean continueQuery = false;
  private StringBuffer conline = new StringBuffer();

  //other strings needed
  private final String LINEEND = ";";

  //database settings history
  private String[] lastDBSettings = new String[8];
  private final int HOSTITER = 0;
  private final int DBTYPEITER = 1;
  private final int DRIVERITER = 2;
  private final int PORTITER = 3;
  private final int USERITER = 4;
  private final int PASSITER = 5;
  private final int DBNAMEITER = 6;
  private final int SOURCEKEYITER = 7;
}