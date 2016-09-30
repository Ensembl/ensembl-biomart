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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterPage;
import org.ensembl.mart.lib.config.RegistryDSConfigAdaptor;
//import org.gnu.readline.Readline;
//import org.gnu.readline.ReadlineCompleter;
import jline.*;
/**
 * <p>ReadlineCompleter implimenting object allowing Mart - specific
 * behavior.  It provides command-completion choices that are based
 * on the position within an MQL command (see MartShellLib for a description
 * of the Mart Query Language) where a user requests completion.
 * It provides public methods to allow its completion mode to
 * be changed, and allow these modes to be backed with specific lists of
 * choices for completion. The system provides the following modes:</p>
 * <ul>
 * <li><p>Command Mode: keyword: "command". Only available MartShell commands, and other names added by the client for this mode, are made available.</p>
 * <li><p>Describe Mode: keyword: "describe". This is a special mode, and requires a Map be added, which further categorizes the describe system commands based on successive keys.</p>
 * <li><p>Help Mode: keyword: "help". Only names added by the client for this mode are made available.</p>
 * <li><p>Get Mode: keyword: "get".  Only attribute_names, and other names added by the client for this mode are made available.</p>
 * <li><p>Sequence Mode: keyword: "sequence".  Only names added by the client for this mode are made available.</p>
 * <li><p>DatasetConfig Mode: keyword: "datasets".  Only dataset names, and other names added by the client for this mode, are made available.</p>
 * <li><p>Where Mode: keyword: "where".  Only filter_names, filter_set_names, and other names added by the client for this mode, are made available.</p>
 * </ul>
 * <br>
 * <p>In some cases it will switch its own mode, but
 * the client code should manage its mode, as well, otherwise it may get stuck
 * in a particular mode when its keywords are processed in a line previous to the current
 * Readline line buffer.</p>
 * <p> Users wishing to add new Domain Specific attributes (analogous to sequences in the present system) should add keywords and modes to the system to
 *  support them.</p>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see MartShellLib
 * @see org.ensembl.mart.lib.config.MartConfiguration
 */
//public class MartCompleter implements ReadlineCompleter {
public class MartCompleter implements Completor {
  /* (non-Javadoc)
   * @see org.gnu.readline.ReadlineCompleter#completer(java.lang.String, int)
   */

  private Iterator possibleValues; // iterator for subsequent calls.
  private SortedSet currentSet = new TreeSet();
  private MartShellLib msl = null;

  private SortedSet commandSet = new TreeSet(); // will hold basic shell commands
  private SortedSet helpSet = new TreeSet(); // will hold help keys available

  private SortedSet addBaseSet = new TreeSet(); // will hold add completions
  private SortedSet listSet = new TreeSet(); // will hold list request keys
  private SortedSet procSet = new TreeSet(); // will hold stored procedure names for remove, describe, and execute
  private SortedSet environmentSet = new TreeSet(); // will hold environment command completions

  private SortedSet removeBaseSet = new TreeSet(); // will hold remove base completions
  private SortedSet updateBaseSet = new TreeSet(); // will hold update remove base completions
  private SortedSet setBaseSet = new TreeSet(); // will hold set base completions
  private SortedSet describeBaseSet = new TreeSet(); // will hold describe base completions
  private SortedSet executeBaseSet = new TreeSet(); // will hold execute base completions

  private SortedSet martSet = new TreeSet(); // will hold  String names for remove, and set 
  private SortedSet adaptorLocationSet = new TreeSet(); // will hold adaptor names for update and remove
  private SortedSet datasetConfigSet = new TreeSet();
  // will hold DatasetConfig names for use, set, remove, and describe
  

  private final List NODATASETWARNING =
    Collections.unmodifiableList(Arrays.asList(new String[] { "No DatasetConfigs loaded", "!" }));
  private final List NOENVWARNING =
    Collections.unmodifiableList(Arrays.asList(new String[] { "Please set environmenal Mart and Dataset", "!" }));
  private final List ERRORMODE = Collections.unmodifiableList(Arrays.asList(new String[] { "ERROR ENCOUNTERED" }));

  private final String ADDC = "add";
  private final String REMOVEC = "remove";
  private final String LISTC = "list";
  private final String UPDATEC = "update";
  private final String SETC = "set";
  private final String UNSETC = "unset";
  private final String DESCRIBEC = "describe";
  private final String HELPC = "help";
  private final String USEC = "use";
  private final String ENVC = "environment";
  private final String EXECC = "execute";
  private final String COUNTFOCUSC = "count_focus_from";
  private final String COUNTROWSC = "count_rows_from";

  private List currentApages = new ArrayList();
  private List currentFpages = new ArrayList();
  private String lastFilterName = null;
  private String lastAttributeName = null;

  private boolean attributeMode = false;
  private boolean whereMode = false;
  private boolean whereNamesMode = false;
  private boolean whereQualifiersMode = false;
  private boolean whereValuesMode = false;

  //listlevel characters
  public int listLevel = 0;
  private String lastLine = null;

  //////// New Declarataions
  
  	public Completor []			completors;
	public final ArgumentDelimiter		delim;
	public boolean						strict = true;

  ///////////////////////////
  private Logger logger = Logger.getLogger(MartCompleter.class.getName());

  /**
   * Creates a MartCompleter Object.  The MartCompleter processes the MartConfiguration
   * object, and stores important internal_names into the completion sets that are applicable to the given MartConfiguration object.
   * @param adaptorManager - a MartConfiguration Object
   */
  public MartCompleter (final List completors)
  {
		this ((Completor [])completors.toArray (new Completor [completors.size ()]));
  }
  public MartCompleter (final Completor completor)
	{
		this (new Completor [] { completor });
	}


	/**
	 *  Constuctor: create a new completor with the default
	 *  argument separator of " ".
	 *
	 *  @param  completors  the embedded argument completors
	 */
	public MartCompleter (final Completor [] completors)
	{
		this (completors, new WhitespaceArgumentDelimiter ());
	}


	/**
	 *  Constuctor: create a new completor with the specified
	 *  argument delimiter.
	 *
	 *  @param  completor	the embedded completor
	 *  @param  delim		the delimiter for parsing arguments
	 */
	public MartCompleter (final Completor completor,
		final ArgumentDelimiter delim)
	{
		this (new Completor [] { completor }, delim);
	}


	/**
	 *  Constuctor: create a new completor with the specified
	 *  argument delimiter.
	 *
	 *  @param  completors	the embedded completors
	 *  @param  delim		the delimiter for parsing arguments
	 */
	public MartCompleter (final Completor [] completors,
		final ArgumentDelimiter delim)
	{
		this.completors = completors;
		this.delim = delim;
	}


	/**
	 *  If true, a completion at argument index N will only succeed
	 *  if all the completions from 0-(N-1) also succeed.
	 */
	public void setStrict (final boolean strict)
	{
		this.strict = strict;
	}


	/**
	 *  Returns whether a completion at argument index N will succees
	 *  if all the completions from arguments 0-(N-1) also succeed.
	 */
	public boolean getStrict ()
	{
		return this.strict;
	}


	
	public static interface ArgumentDelimiter
	{
		ArgumentList delimit (String buffer, int argumentPosition);

		boolean isDelimiter (String buffer, int pos);
	}


	public static abstract class AbstractArgumentDelimiter
		implements ArgumentDelimiter
	{
		private char [] quoteChars = new char [] { '\'', '"' };
		private char [] escapeChars = new char [] { '\\' };


		public void setQuoteChars (final char [] quoteChars)
		{
			this.quoteChars = quoteChars;
		}


		public char [] getQuoteChars ()
		{
			return this.quoteChars;
		}


		public void setEscapeChars (final char [] escapeChars)
		{
			this.escapeChars = escapeChars;
		}


		public char [] getEscapeChars ()
		{
			return this.escapeChars;
		}



		public ArgumentList delimit (final String buffer, final int cursor)
		{
			List args = new LinkedList ();
			StringBuffer arg = new StringBuffer ();
			int argpos = -1;
			int bindex = -1;

			for (int i = 0; buffer != null && i <= buffer.length (); i++)
			{
				// once we reach the cursor, set the
				// position of the selected index
				if (i == cursor)
				{
					bindex = args.size ();
					// the position in the current argument is just the
					// length of the current argument
					argpos = arg.length ();
				}

				if (i == buffer.length () || isDelimiter (buffer, i))
				{
					if (arg.length () > 0)
					{
						args.add (arg.toString ());
						arg.setLength (0); // reset the arg
					}
				}
				else
				{
					arg.append (buffer.charAt (i));
				}
			}

			return new ArgumentList (
				(String [])args.toArray (new String [args.size ()]),
				bindex, argpos, cursor);
		}


		public boolean isDelimiter (final String buffer, final int pos)
		{
			if (isQuoted (buffer, pos))
				return false;
			if (isEscaped (buffer, pos))
				return false;

			return isDelimiterChar (buffer, pos);
		}


		public boolean isQuoted (final String buffer, final int pos)
		{
			return false;
		}


		public boolean isEscaped (final String buffer, final int pos)
		{
			if (pos <= 0)
				return false;

			for (int i = 0; escapeChars != null && i < escapeChars.length; i++)
			{
				if (buffer.charAt (pos) == escapeChars [i])
					return !isEscaped (buffer, pos - 1); // escape escape
			}

			return false;
		}


		public abstract boolean isDelimiterChar (String buffer, int pos);
	}

	public static class WhitespaceArgumentDelimiter
		extends AbstractArgumentDelimiter
	{
	
		public boolean isDelimiterChar (String buffer, int pos)
		{
			return Character.isWhitespace (buffer.charAt (pos));
		}
	}


	
	public static class ArgumentList
	{
		private String [] arguments;
		private int cursorArgumentIndex;
		private int argumentPosition;
		private int bufferPosition;
		public ArgumentList (String [] arguments, int cursorArgumentIndex,
			int argumentPosition, int bufferPosition)
		{
			this.arguments = arguments;
			this.cursorArgumentIndex = cursorArgumentIndex;
			this.argumentPosition = argumentPosition;
			this.bufferPosition = bufferPosition;
		}


		public void setCursorArgumentIndex (int cursorArgumentIndex)
		{
			this.cursorArgumentIndex = cursorArgumentIndex;
		}


		public int getCursorArgumentIndex ()
		{
			return this.cursorArgumentIndex;
		}


		public String getCursorArgument ()
		{
			if (cursorArgumentIndex < 0
				|| cursorArgumentIndex >= arguments.length)
				return null;

			return arguments [cursorArgumentIndex];
		}


		public void setArgumentPosition (int argumentPosition)
		{
			this.argumentPosition = argumentPosition;
		}


		public int getArgumentPosition ()
		{
			return this.argumentPosition;
		}


		public void setArguments (String [] arguments)
		{
			this.arguments = arguments;
		}


		public String [] getArguments ()
		{
			return this.arguments;
		}


		public void setBufferPosition (int bufferPosition)
		{
			this.bufferPosition = bufferPosition;
		}


		public int getBufferPosition ()
		{
			return this.bufferPosition;
		}
	}
		
  
  public void setController(MartShellLib msl) throws ConfigurationException {
    this.msl = msl;
    RegistryDSConfigAdaptor adaptorManager = msl.adaptorManager;
    if (adaptorManager.getDatasetNames(false).length > 0) {
      String[] dsets = adaptorManager.getDatasetNames(false);

      for (int i = 0, n = dsets.length; i < n; i++) {
        String dataset = dsets[i];
        datasetConfigSet.addAll(Arrays.asList(adaptorManager.getDatasetConfigInternalNamesByDataset(dataset)));
      }
    }
  }

  /**
   * Implimentation of the ReadlineCompleter completer method.  Switches its state based on the presence and position of keywords in the command.
   * <ul>
   *   <li><p> If the word "describe" occurs, Describe Mode is chosen.</p>
   *   <li><p> If the word "help" occurs, Help Mode is chosen.</p> 
   *   <li><p> If the word "select" is present, and occurs after all other keywords present, then Select Mode is chosen.</p>
   *   <li><p> If the word "sequence" is present, and occurs after all other keywords present, then Sequence Mode is chosen.</p>
   *   <li><p> If the word "from" is present, and occurs after all other keywords present, then From Mode is chosen.</p>
   *   <li><p> If the word "where" is present, and occurs after all other keywords present, then Where Mode is chosen.</p>
   * </ul>
   */
  /*
  public String completer(String text, int state) {
    if (state == 0) {
      // first call to completer(): initialize our choices-iterator
    	//System.out.println("*STATE = 0*" + text);
    	setModeForLine(Readline.getLineBuffer());
      possibleValues = currentSet.tailSet(text).iterator();
    }
    
    
  //  Iterator temp= possibleValues;
   // while(temp.hasNext())
   // {
    //	System.out.println("**: " + ((String) temp.next()));    	
   // }   
    
    if (possibleValues.hasNext()) {
      String nextKey = (String) possibleValues.next();
      if (nextKey.startsWith(text))
        return nextKey;
    }

    return null; // we reached the last choice.
  }
*/
  public int complete (final String buffer, final int cursor,	final List candidates)
  {
	  //	 TODO: Copy code from above and create a new SimpleCompletor each time as in your own example.
	  ArgumentList list = delim.delimit (buffer, cursor);
	  int argpos = list.getArgumentPosition ();
	  int argIndex = list.getCursorArgumentIndex ();
	  if (argIndex < 0)
		   return -1;

	  Completor comp;
		
	  setModeForLine(buffer);
	  
	  //////////// For making possible Values as variable possibleOptions String [] for SimpleCompletor
	  Iterator temp = currentSet.iterator();
	  List mylist = new LinkedList();
	  while(temp.hasNext())
	  {
		  mylist.add(temp.next());
	  }	  
	  String [] possibleOptions = new String [mylist.size()];
	  mylist.toArray(possibleOptions);
	  /////////////////////////////////////////////////////////////////////
	  
	  comp = new SimpleCompletor (possibleOptions);
		int ret = comp.complete (list.getCursorArgument (), argpos, candidates);
		if (ret == -1)
			return -1;

		int pos = ret + (list.getBufferPosition () - argpos) + 1;

		if (cursor != buffer.length () && delim.isDelimiter (buffer, cursor))
		{
			for (int i = 0; i < candidates.size (); i++)
			{
				String val = candidates.get (i).toString ();
				while (val.length () > 0 &&
					delim.isDelimiter (val, val.length () - 1))
					val = val.substring (0, val.length () - 1);

				candidates.set (i, val);
			}
		}

		ConsoleReader.debug ("Completing " + buffer + "(pos=" + cursor + ") "
			+ "with: " + candidates + ": offset=" + pos);

		return pos;	  
  }
  
  
  public void setModeForLine(String currentCommand) {
    if (lastLine == null || !(lastLine.equals(currentCommand))) {
      if (currentCommand.startsWith(COUNTFOCUSC))
        currentCommand = currentCommand.substring(COUNTFOCUSC.length()).trim();
      if (currentCommand.startsWith(COUNTROWSC))
        currentCommand = currentCommand.substring(COUNTROWSC.length()).trim();

      currentCommand = currentCommand.trim(); //strip off trailing whitespace, so most modes will set empty mode when things match exactly
      
      if (currentCommand.startsWith(ADDC))
        setAddMode(currentCommand);
      else if (currentCommand.startsWith(REMOVEC))
        setRemoveMode(currentCommand);
      else if (currentCommand.startsWith(LISTC))
        setListMode(currentCommand);
      else if (currentCommand.startsWith(UPDATEC))
        setUpdateMode(currentCommand);
      else if (currentCommand.startsWith(SETC) || currentCommand.startsWith(UNSETC))
        setSetUnsetMode(currentCommand);
      else if (currentCommand.startsWith(DESCRIBEC))
        setDescribeMode(currentCommand);
      else if (currentCommand.startsWith(ENVC))
        setEnvironmentMode();
      else if (currentCommand.startsWith(EXECC))
        setExecuteMode(currentCommand);
      else if (currentCommand.startsWith(HELPC))
        setHelpMode();
      else if (currentCommand.startsWith(USEC)) {
        if (currentCommand.endsWith(">"))
          setMartReqMode();
        else
          setUseDatasetMode(currentCommand);
      } else {
        int usingInd = currentCommand.lastIndexOf(MartShellLib.USINGQSTART);

        if (usingInd >= 0) {
          msl.usingLocalDataset = true;

          String[] toks = currentCommand.split("\\s+");

          //unset all modes if user has erased back to using
          if (toks.length < 3) {
            attributeMode = false;
            whereMode = false;
          }

          if (toks.length >= 2) {
            String datasetreq = toks[1];

            if (datasetreq.indexOf(">") > 0)
              datasetreq = datasetreq.substring(0, datasetreq.indexOf(">"));

            try {
              msl.setLocalDatasetFor(datasetreq);
            } catch (InvalidQueryException e) {
              //ignore this.  If they have specified a strange dataset, and then do get, it will complain              
            }
          }
        }

        String[] lineWords = currentCommand.split(" "); // split on single space

        // determine which mode to be in during a query
        int getInd = currentCommand.lastIndexOf(MartShellLib.GETQSTART);
        int seqInd = currentCommand.lastIndexOf(MartShellLib.QSEQUENCE);
        int whereInd = currentCommand.lastIndexOf(MartShellLib.QWHERE);
        int limitInd = currentCommand.lastIndexOf(MartShellLib.QLIMIT);

        if ((usingInd > seqInd) && (usingInd > getInd) && (usingInd > whereInd) && (usingInd > limitInd)) {
          if (currentCommand.endsWith(">"))
            setMartReqMode();
          else
            setUseDatasetMode(currentCommand);
        }

        if ((seqInd > usingInd) && (seqInd > getInd) && (seqInd > whereInd) && (seqInd > limitInd))
          setDomainSpecificMode();

        if ((getInd > usingInd) && (getInd > seqInd) && (getInd > whereInd) && (getInd > limitInd))
          attributeMode = true;

        if ((whereInd > usingInd) && (whereInd > seqInd) && (whereInd > getInd) && (whereInd > limitInd)) {
          attributeMode = false;
          whereMode = true;
        }

        if ((limitInd > usingInd) && (limitInd > getInd) && (limitInd > seqInd) && (limitInd > whereInd)) {
          attributeMode = false;
          whereMode = false;
          setEmptyMode();
        }

        // if none of the key placeholders are present, may still need to further refine the mode
        if (attributeMode) {
          if (lineWords.length > 0) {
            String lastWord = lineWords[lineWords.length - 1];

            if (lastWord.equals(MartShellLib.GETQSTART)) {
              lastAttributeName = null;
              currentApages = new ArrayList();
            } else {
              if (lastWord.endsWith(",")) {
                lastAttributeName = lastWord.substring(0, lastWord.length() - 1);
                pruneAttributePages();
              }
            }

            setAttributeNames();
          }
        }

        if (whereMode) {
          if (!(whereNamesMode || whereQualifiersMode || whereValuesMode)) {
            //first time in
            whereNamesMode = true;
            whereQualifiersMode = false;
            whereValuesMode = true;
            currentFpages = new ArrayList();
            setWhereNames();
          }

          if (lineWords.length > 0) {
            String lastWord = lineWords[lineWords.length - 1];

            if (lastWord.equals(MartShellLib.QWHERE)) {
              lastFilterName = null;
              whereNamesMode = true;
              whereQualifiersMode = false;
              whereValuesMode = true;
              setWhereNames();
            } else if (MartShellLib.ALLQUALIFIERS.contains(lastWord)) {
              if (lineWords.length > 1) {
                lastFilterName = lineWords[lineWords.length - 2];
                pruneFilterPages();
              }

              if (MartShellLib.BOOLEANQUALIFIERS.contains(lastWord)) {
                whereNamesMode = false;
                whereQualifiersMode = true;
                whereValuesMode = false;
                setEmptyMode();
              } else {
                whereNamesMode = false;
                whereQualifiersMode = false;
                whereValuesMode = true;
                setWhereValues(lastWord);
              }
            }

            if (whereNamesMode) {
              if (msl.localDataset != null) {
                if (msl.localDataset.containsFilterDescription(lastWord)) {
                  String thisField = msl.localDataset.getFilterDescriptionByInternalName(lastWord).getField(lastWord);

                  if (thisField != null && thisField.length() > 0) {
                    lastFilterName = lastWord;
                    pruneFilterPages();

                    whereNamesMode = false;
                    whereQualifiersMode = true;
                    whereValuesMode = false;
                    setWhereQualifiers();
                  }
                }
              } else if (!msl.usingLocalDataset && msl.envDataset != null) {
                if (msl.envDataset.containsFilterDescription(lastWord)) {
                  FilterDescription thisFilter = msl.envDataset.getFilterDescriptionByInternalName(lastWord);
                  String thisField = thisFilter.getField(lastWord);

                  if (thisField != null && thisField.length() > 0) {
                    lastFilterName = lastWord;
                    pruneFilterPages();

                    logger.info(lastWord + " appears to be a filter, going to whereQualifiersMode\n");

                    whereNamesMode = false;
                    whereQualifiersMode = true;
                    whereValuesMode = false;
                    setWhereQualifiers();
                  }
                }
              } else
                setNoDatasetConfigMode();
            } else if (whereQualifiersMode) {
              if (MartShellLib.ALLQUALIFIERS.contains(lastWord)) {
                if (lineWords.length > 1) {
                  lastFilterName = lineWords[lineWords.length - 2];
                  pruneFilterPages();
                }

                if (MartShellLib.BOOLEANQUALIFIERS.contains(lastWord)) {
                  setEmptyMode();
                } else {
                  whereQualifiersMode = false;
                  whereValuesMode = true;
                  setWhereValues(lastWord);
                }
              } else if (lastWord.equalsIgnoreCase(MartShellLib.FILTERDELIMITER)) {
                whereNamesMode = true;
                whereQualifiersMode = false;
                whereValuesMode = false;
                pruneFilterPages();
                setWhereNames();
              }
            } else if (whereValuesMode) {
              if (lastWord.equalsIgnoreCase(MartShellLib.FILTERDELIMITER)) {
                whereNamesMode = true;
                whereQualifiersMode = false;
                whereValuesMode = false;
                pruneFilterPages();
                setWhereNames();
              }
            }
          }
        }
      }

      lastLine = currentCommand;
      
    }
  }

  /**
   * Sets the MartCompleter into COMMAND mode
   *
   */
  public void setCommandMode() {
    currentSet = new TreeSet();
    currentSet.addAll(commandSet);

    // reset state to pristine
    setNoLocalDataset();
    currentApages = new ArrayList();
    currentFpages = new ArrayList();
    lastFilterName = null;
    lastAttributeName = null;
    attributeMode = false;
    whereMode = false;
    whereNamesMode = false;
    whereQualifiersMode = false;
    whereValuesMode = false;
    lastLine = null;
  }

  private void setNoLocalDataset() {
    msl.localDataset = null;
    msl.usingLocalDataset = false;
  }

  private void setHelpMode() {
    currentSet = new TreeSet();
    currentSet.addAll(helpSet);
  }

  private void setListMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length <= 1) {
      setListBaseMode();
    } else {
      if (toks[1].equalsIgnoreCase("datasets") || toks[1].equalsIgnoreCase("datasetconfigs"))
        setAllAdaptorLocationMode();
    }
  }

  private void setListBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(listSet);
  }

  private void setAddMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length == 1)
      setAddBaseMode();
    else {
      if (toks[1].equalsIgnoreCase("Datasets")) {
        if (toks.length == 3 && toks[2].equalsIgnoreCase("from"))
          setEmptyMode();
        else
          setFromMode();
      } else {
        if (addBaseSet.contains(toks[1]))
          setEmptyMode();
      }
    }
  }

  private void setAddBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(addBaseSet);
  }

  private void setEnvironmentMode() {
    currentSet = new TreeSet();
    currentSet.addAll(environmentSet);
  }

  private void setRemoveMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length == 1)
      setRemoveBaseMode();
    else if (toks.length == 2) {
      String request = toks[1];

      if (request.equalsIgnoreCase("Mart"))
        setMartReqMode();
      else if (request.equalsIgnoreCase("Datasets"))
        setFromMode();
      else if (request.equalsIgnoreCase("Dataset"))
        setDatasetReqMode();
      else if (request.equalsIgnoreCase("DatasetConfig"))
        setDatasetConfigReqMode();
      else if (request.equalsIgnoreCase("Procedure"))
        setProcedureNameMode();
    } else {
      if (toks.length == 3 && toks[2].equalsIgnoreCase("from"))
        setMartReqMode();
    }
  }

  private void setRemoveBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(removeBaseSet);
  }

  private void setUpdateMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length == 1)
      setUpdateBaseMode();
    else if (toks.length == 2) {
      String request = toks[1];

      if (request.equalsIgnoreCase("Datasets"))
        setFromMode();
      else {
        if (request.equalsIgnoreCase("Dataset"))
          setDatasetReqMode();
      }
    } else {
      if (toks.length == 3 && toks[2].equalsIgnoreCase("from"))
        setMartReqMode();
    }
  }

  private void setUpdateBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(updateBaseSet);
  }

  private void setSetUnsetMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length <= 1)
      setSetBaseMode();
    else {
      try {
        if (toks[0].equals(SETC)) {
          if (toks[1].equalsIgnoreCase("mart")) {
            if (toks.length == 3 && msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName(toks[2]) ) )
              setEmptyMode();
            else
              setMartReqMode();
          } else if (toks[1].equalsIgnoreCase("dataset")) {
            if (toks.length == 3 && msl.adaptorManager.supportsDataset(toks[2]))
              setEmptyMode();
            else
              setDatasetReqMode();
          } else if (toks.length < 3 && setBaseSet.contains(toks[1]))
            setEmptyMode();
        } else {
          if (toks.length == 2 && setBaseSet.contains(toks[1]))
            setEmptyMode();
        }
      } catch (ConfigurationException e) {
        setErrorMode("Caught ConfigurationException updating Completion System\n");
      }
    }
  }

  private void setSetBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(setBaseSet);
  }

  private void setExecuteMode(String token) {
    String[] toks = token.split("\\s+");

    if (toks.length == 1)
      setExecuteBaseMode();
    else if (toks.length == 2) {
      if (toks[1].equalsIgnoreCase("Procedure"))
        setProcedureNameMode();
      else {
        if (executeBaseSet.contains(toks[1]))
          setEmptyMode();
      }
    }
  }

  private void setExecuteBaseMode() {
    currentSet = new TreeSet();
    currentSet.addAll(executeBaseSet);
  }

  private void setDescribeMode(String token) {
    String[] toks = token.split("\\s+");
    if (toks.length == 1)
      setBaseDescribeMode();
    else if (toks.length >= 2) {
      String request = toks[1];

      try {
        if (request.equalsIgnoreCase("dataset")) {
          if (toks.length == 3 && msl.adaptorManager.supportsDataset(toks[2]))
            setEmptyMode();
          else
            setDatasetReqMode();
        } else if (request.equalsIgnoreCase("Mart")) {
          if (toks.length == 3 && msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( toks[2] ) ) )
            setEmptyMode();
          else
            setMartReqMode();
        } else if (request.equalsIgnoreCase("filter"))
          setDescribeFilterMode();
        else if (request.equalsIgnoreCase("attribute"))
          setDescribeAttributeMode();
        else {
          if (request.equalsIgnoreCase("procedure")) {
            setDescribeProcedureMode();
          }
        }
      } catch (ConfigurationException e) {
        setErrorMode("Caught ConfigurationException updating Completion System\n");
      }
    }
  }

  private void setBaseDescribeMode() {
    currentSet = new TreeSet();
    currentSet.addAll(describeBaseSet);
  }

  private void setDatasetReqMode() {
    if (msl.envMart == null)
      setNoEnvMode();
    else
      setDatasetReqMode(msl.envMart.getName());
  }

  private void setDatasetReqMode(String martName) {
    currentSet = new TreeSet();
    try {
      if (msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) ) )
        currentSet.addAll(Arrays.asList(msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).getDatasetNames(false)));
    } catch (ConfigurationException e) {
      currentSet = new TreeSet();
      setErrorMode("Couldng set describe dataset mode, caught Configuration Exception: " + e.getMessage() + "\n");
    }
  }

  private void setUsingDatasetReqMode(String martName) {
    currentSet = new TreeSet();
    try {
      if (msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) )) {
        String[] dsets = msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).getDatasetNames(false);
        for (int i = 0, n = dsets.length; i < n; i++) {
          String dset = dsets[i];
          currentSet.add(martName+"."+dset); //martName.dataset
        } 
      }
    } catch (ConfigurationException e) {
      currentSet = new TreeSet();
      setErrorMode("Couldnot set describe dataset mode, caught Configuration Exception: " + e.getMessage() + "\n");
    }
  }
  
  private void setDatasetConfigReqMode() {
    currentSet = new TreeSet();
    if (msl.envMart == null)
      setNoEnvMode();
    else {
      if (msl.envDataset == null)
        setNoEnvMode();
      else
        setDatasetConfigReqMode(msl.envMart.getName(), msl.envDataset.getDataset());
    }
  }

  private void setDatasetConfigReqMode(String martName, String datasetName) {
    currentSet = new TreeSet();
    
    try {
      if (msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) )
        && msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).supportsDataset(datasetName))
        currentSet.addAll(
          Arrays.asList(
            msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).getDatasetConfigInternalNamesByDataset(datasetName)));
    } catch (ConfigurationException e) {
      currentSet = new TreeSet();
      if (logger.isLoggable(Level.INFO))
        logger.info("Couldng set dataset config req mode, caught Configuration Exception: " + e.getMessage() + "\n");
    }
  }

  private void setUsingDatasetConfigReqMode(String martName, String datasetName) {
    currentSet = new TreeSet();
    
    try {
      if (msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) ) 
          && msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).supportsDataset(datasetName)) {
        String[] dnames = msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName )).getDatasetConfigInternalNamesByDataset(datasetName);
        for (int i = 0, n = dnames.length; i < n; i++) {
          String dname = dnames[i];
          currentSet.add(martName+"."+datasetName+"."+dname);
        }
      }       
    } catch (ConfigurationException e) {
      currentSet = new TreeSet();
      if (logger.isLoggable(Level.INFO))
        logger.info("Couldng set dataset config req mode, caught Configuration Exception: " + e.getMessage() + "\n");
    }
  }
  
  private void setDescribeFilterMode() {
    if (msl.envDataset == null)
      setNoEnvMode();
    else {
      currentSet = new TreeSet();
      currentSet.addAll(msl.envDataset.getFilterCompleterNames());
    }
  }

  private void setDescribeAttributeMode() {
    if (msl.envDataset == null)
      setNoEnvMode();
    else {
      currentSet = new TreeSet();
      currentSet.addAll(msl.envDataset.getAttributeCompleterNames());
    }
  }

  private void setDescribeProcedureMode() {
    currentSet = new TreeSet();
    currentSet.addAll(procSet);
  }

  private void setAttributeNames() {
    if (msl.localDataset != null) {
      currentSet = new TreeSet();

      if (currentApages.size() == 0)
        currentApages = Arrays.asList(msl.localDataset.getAttributePages());

      for (int i = 0, n = currentApages.size(); i < n; i++) {
        AttributePage apage = (AttributePage) currentApages.get(i);

        List completers = apage.getCompleterNames();
        for (int j = 0, m = completers.size(); j < m; j++) {
          String completer = (String) completers.get(j);
          if (!currentSet.contains(completer))
            currentSet.add(completer);
        }
      }
    } else if (!(msl.usingLocalDataset) && msl.envDataset != null) {
      currentSet = new TreeSet();

      if (currentApages.size() == 0)
        currentApages = Arrays.asList(msl.envDataset.getAttributePages());
      for (int i = 0, n = currentApages.size(); i < n; i++) {
        AttributePage apage = (AttributePage) currentApages.get(i);

        List completers = apage.getCompleterNames();
        for (int j = 0, m = completers.size(); j < m; j++) {
          String completer = (String) completers.get(j);
          if (!currentSet.contains(completer))
            currentSet.add(completer);
        }
      }
    } else
      setNoDatasetConfigMode();
  }

  private void setFromMode() {
    currentSet = new TreeSet();
    currentSet.add("from");
  }

  private void setEmptyMode() {
    currentSet = new TreeSet();
  }

  private void setNoDatasetConfigMode() {
    currentSet = new TreeSet();
    currentSet.addAll(NODATASETWARNING);
  }

  private void setNoEnvMode() {
    currentSet = new TreeSet();
    currentSet.addAll(NOENVWARNING);
  }

  private void setErrorMode(String error) {
    currentSet = new TreeSet();
    currentSet.addAll(ERRORMODE);
    currentSet.add(error);
  }

  private void pruneAttributePages() {
    List newPages = new ArrayList();
    if (msl.localDataset != null)
      newPages = msl.localDataset.getPagesForAttribute(lastAttributeName);
    else if (!msl.usingLocalDataset && msl.envDataset != null)
      newPages = msl.envDataset.getPagesForAttribute(lastAttributeName);
    else
      newPages = new ArrayList();

    if (newPages.size() < currentApages.size())
      currentApages = new ArrayList(newPages);
  }

  private void setDomainSpecificMode() {
    attributeMode = false;
    currentSet = new TreeSet();
    
    //find all sequence attributes
    AttributePage seqPage = null;
    
    if (msl.usingLocalDataset)
      seqPage = msl.localDataset.getAttributePageByInternalName("sequences");
    else
      seqPage = msl.envDataset.getAttributePageByInternalName("sequences");
    
    if (seqPage == null) {
      setErrorMode("No sequences loaded in dataset " + msl.envDataset.getDisplayName() + "\n");
      return;
    }
    
    AttributeGroup seqGroup = (AttributeGroup) seqPage.getAttributeGroupByName("sequence");
    
    if (seqGroup == null) {
        setErrorMode("No sequences loaded in dataset " + msl.envDataset.getDisplayName() + "\n");
        return;
    }
    
    AttributeCollection[] cols = seqGroup.getAttributeCollections();
    AttributeCollection seqCol = null;
    
    for (int i = 0; i < cols.length; i++) {
      if (cols[i].getInternalName().matches("\\w*seq_scope\\w*")) {
        seqCol = cols[i];
        break;
      }
    }
    
    if (seqCol == null) {
        setErrorMode("No sequences loaded in dataset " + msl.envDataset.getDisplayName() + "\n");
        return;
    }
    
    currentSet.addAll(seqCol.getHiddenCompleterNames());
  }

  private void setAllAdaptorLocationMode() {
    currentSet = new TreeSet();
    currentSet.add("all");
    try {
      String[] names =  msl.adaptorManager.getAdaptorNames();
      for (int i = 0, n = names.length; i < n; i++) {
	    currentSet.add( msl.canonicalizeMartName( names[i] ) );
	  }
    } catch (ConfigurationException e) {
      if (logger.isLoggable(Level.INFO))
        logger.info("Caught ConfigurationException getting adaptor names: " + e.getMessage() + "\n");
    }
  }

  private void setMartReqMode() {
    currentSet = new TreeSet();
    currentSet.addAll(martSet);
  }

  private void setUseDatasetMode(String token) {
    try {
      Pattern usePat = Pattern.compile(USEC + "\\s*(\\w*)(\\.?)(\\w*)(\\.?)(\\w*)");
      Matcher useMatcher = usePat.matcher(token);
      
      Pattern usingPat = Pattern.compile(MartShellLib.USINGQSTART + "\\s*(\\w*)(\\.?)(\\w*)(\\.?)(\\w*)");
      Matcher usingMatcher = usingPat.matcher(token);
      
      if (useMatcher.matches())
        setUseDatasetModeWithMatcher(useMatcher);
      else if (usingMatcher.matches())
        setUseDatasetModeWithMatcher(usingMatcher);
      
    } catch (ConfigurationException e) {
      setErrorMode("Caught ConfigurationException updating the Completion System\n");
    }
  }

  private void setUseDatasetModeWithMatcher(Matcher matcher) throws ConfigurationException {
    String mat1 = matcher.group(1);
    String mat2 = matcher.group(2);
    String mat3 = matcher.group(3);
    String mat4 = matcher.group(4);
    String mat5 = matcher.group(5);
        
    if (validString(mat5)) {
      //using x.y.z
      if (validMartAndDataset(mat1, mat3)) {
          if (msl.adaptorManager.getDatasetConfigByDatasetInternalName(mat3, mat5) != null)
            setEmptyMode(); //nothing more to complete
          else
            setUsingDatasetConfigReqMode(mat1, mat3); //still in configreq
      }            
    } else if (validString(mat4)) {
      //using x.y.
      if (validMartAndDataset(mat1, mat3))
        setUsingDatasetConfigReqMode(mat1, mat3);
    } else if (validString(mat3)) {
      //using x.y
      if (validMartAndDataset(mat1, mat3))
        setEmptyMode(); //nothing more to complete
      else if (validMart(mat1))
        setUsingDatasetReqMode(mat1);
      else if (msl.envMart != null)
        if (validDatasetForMart(msl.envMart.getName(), mat3))
          setEmptyMode(); //nothing more to complete
        else
          setDatasetReqMode(msl.envMart.getName());
      else
        setNoEnvMode();
    } else if (validString(mat2)) {
      //using x.
      if (validMart(mat1))
        setUsingDatasetReqMode(mat1);
      else if (msl.envMart != null) {
        if (validDatasetForMart(msl.envMart.getName(), mat1))
          setEmptyMode();
        else
          setDatasetReqMode(msl.envMart.getName());
      } else
        setNoEnvMode();
    } else if (validString(mat1)) {
      //using x
      if (validMart(mat1))
        setEmptyMode(); //nothing more to complete
      else if (msl.envMart != null) {
        //assume dataset relative to envMart
        setDatasetReqMode(msl.envMart.getName());
      } else {
        //assume mart. request
        setMartReqMode();
      }
    } else {
      //using
      if (msl.envMart != null)
        setDatasetReqMode(msl.envMart.getName());
      else
        setMartReqMode();
    }
  }
  
  private void setProcedureNameMode() {
    currentSet = new TreeSet();
    currentSet.addAll(procSet);
  }

  private void setWhereNames() {
    if (msl.localDataset != null) {
      currentSet = new TreeSet();

      if (currentFpages.size() == 0)
        currentFpages = Arrays.asList(msl.localDataset.getFilterPages());

      for (int i = 0, n = currentFpages.size(); i < n; i++) {
        FilterPage fpage = (FilterPage) currentFpages.get(i);

        List completers = fpage.getCompleterNames();
        for (int j = 0, m = completers.size(); j < m; j++) {
          String completer = (String) completers.get(j);
          if (!currentSet.contains(completer))
            currentSet.add(completer);
        }
      }
    } else if (!(msl.usingLocalDataset) && msl.envDataset != null) {
      currentSet = new TreeSet();

      if (currentFpages.size() == 0)
        currentFpages = Arrays.asList(msl.envDataset.getFilterPages());

      for (int i = 0, n = currentFpages.size(); i < n; i++) {
        FilterPage fpage = (FilterPage) currentFpages.get(i);

        List completers = fpage.getCompleterNames();
        for (int j = 0, m = completers.size(); j < m; j++) {
          String completer = (String) completers.get(j);
          if (!currentSet.contains(completer))
            currentSet.add(completer);
        }
      }
    } else
      setNoDatasetConfigMode();
  }

  private void setWhereQualifiers() {
    currentSet = new TreeSet();

    if (msl.localDataset != null) {
      if (msl.localDataset.containsFilterDescription(lastFilterName))
        currentSet.addAll(msl.localDataset.getFilterCompleterQualifiersByInternalName(lastFilterName));
    } else if (!msl.usingLocalDataset && msl.envDataset != null) {
      if (logger.isLoggable(Level.INFO))
        logger.info("getting qualifiers for filter " + lastFilterName + "\n");

      if (msl.envDataset.containsFilterDescription(lastFilterName)) {
        if (logger.isLoggable(Level.INFO))
          logger.info("Its a filter, getting from dataset\n");

        currentSet.addAll(msl.envDataset.getFilterCompleterQualifiersByInternalName(lastFilterName));
      }
    } else
      setNoDatasetConfigMode();
  }

  private void setWhereValues(String lastWord) {
    currentSet = new TreeSet();

    if (msl.localDataset != null) {
      if (msl.localDataset.containsFilterDescription(lastFilterName)) {
        currentSet.addAll(msl.localDataset.getFilterCompleterValuesByInternalName(lastFilterName));

        if (lastWord.equalsIgnoreCase("in"))
          currentSet.addAll(procSet);
      }
    } else if (!msl.usingLocalDataset && msl.envDataset != null) {
      if (msl.envDataset.containsFilterDescription(lastFilterName)) {
        currentSet.addAll(msl.envDataset.getFilterCompleterValuesByInternalName(lastFilterName));

        if (lastWord.equalsIgnoreCase("in"))
          currentSet.addAll(procSet);
      }
    } else
      setNoDatasetConfigMode();
  }

  private void pruneFilterPages() {
    List newPages = new ArrayList();

    if (msl.localDataset != null)
      newPages = msl.localDataset.getPagesForFilter(lastFilterName);
    else if (!msl.usingLocalDataset && msl.envDataset != null)
      newPages = msl.envDataset.getPagesForFilter(lastFilterName);
    else
      newPages = new ArrayList();

    if (newPages.size() < currentFpages.size())
      currentFpages = new ArrayList(newPages);
  }

  /**
   * Set the String names (path, url, Mart name) used to refer
   * to locations from whence DSConfigAdaptor objects were loaded.
   * @param names -- names of path, url, or Mart names from whence DSConfigAdaptor Objects were loaded 
   */
  public void setAdaptorLocations(Collection names) {
    adaptorLocationSet = new TreeSet();
    adaptorLocationSet.addAll(names);
  }

  public void setDatasetConfigInternalNames(Collection names) {
    datasetConfigSet = new TreeSet();
    datasetConfigSet.addAll(names);
  }

  /**
   * Set the Names used to refer to Mart Objects in the Shell.
   * @param names -- names used to refer to Mart Objects in the Shell.
   */
  public void setMartNames(Collection names) {
    martSet = new TreeSet();
    martSet.addAll(names);
  }

  /**
   * Set the names of stored procedures
   * @param names -- names of stored procedures
   */
  public void setProcedureNames(Collection names) {
    procSet = new TreeSet();
    procSet.addAll(names);
  }

  /**
   * Set the Base Shell Commands available
   * @param names -- base commands available to the Shell.
   */
  public void setBaseCommands(Collection names) {
    commandSet = new TreeSet();
    commandSet.addAll(names);
  }

  /**
   * set Help commands
   * @param names -- help commands
   */
  public void setHelpCommands(Collection names) {
    helpSet = new TreeSet();
    helpSet.addAll(names);
  }

  /**
   * Set the add command sub tokens
   * @param addRequests -- add command sub tokens
   */
  public void setAddCommands(Collection requests) {
    addBaseSet = new TreeSet();
    addBaseSet.addAll(requests);
  }

  /**
   * Set the Base Remove sub tokens
   * @param removeRequests -- base remove sub tokens
   */
  public void setRemoveBaseCommands(Collection requests) {
    removeBaseSet = new TreeSet();
    removeBaseSet.addAll(requests);
  }

  /**
   * Set the List command sub tokens
   * @param listRequests -- list command sub tokens
   */
  public void setListCommands(Collection requests) {
    listSet = new TreeSet();
    listSet.addAll(requests);
  }

  /**
   * Set the Update command base sub tokens
   * @param updateRequests -- update command base sub tokens
   */
  public void setUpdateBaseCommands(Collection requests) {
    updateBaseSet = new TreeSet();
    updateBaseSet.addAll(requests);
  }

  /**
   * Set the Set command base sub tokens
   * @param requests -- Set command base sub tokens
   */
  public void setSetBaseCommands(Collection requests) {
    setBaseSet = new TreeSet();
    setBaseSet.addAll(requests);
  }

  /**
   * Set the Describe command base sub tokens
   * @param describeRequests -- describe command base sub tokens
   */
  public void setDescribeBaseCommands(Collection requests) {
    describeBaseSet = new TreeSet();
    describeBaseSet.addAll(requests);
  }

  /**
   * Set the Environment command base sub tokens
   * @param envRequests -- environment command base sub tokens
   */
  public void setEnvironmentBaseCommands(Collection requests) {
    environmentSet = new TreeSet();
    environmentSet.addAll(requests);
  }

  /**
   * Sets the Execute command base sub tokens
   * @param executeRequests -- execute command base sub tokens
   */
  public void setExecuteBaseCommands(Collection requests) {
    executeBaseSet = new TreeSet();
    executeBaseSet.addAll(requests);
  }

  private void logInfo(String message) {
    if (logger.isLoggable(Level.INFO))
      logger.info(message);
  }

  private void logWarning(String message) {
    if (logger.isLoggable(Level.WARNING))
      logger.warning(message);
  }
  
  private boolean validString(String input) {
    return input != null && input.length() > 0;
  }
  
  private boolean validMartAndDataset(String martName, String dataset) throws ConfigurationException {
    return msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) ) && msl.adaptorManager.supportsDataset(dataset);
  }
  
  private boolean validDatasetForMart(String martName, String dataset) throws ConfigurationException {
    return msl.adaptorManager.getAdaptorByName( msl.deCanonicalizeMartName( martName ) ).supportsDataset(dataset);
  }
  
  private boolean validMart(String martName) throws ConfigurationException {
    return msl.adaptorManager.supportsAdaptor( msl.deCanonicalizeMartName( martName ) );
  }
}