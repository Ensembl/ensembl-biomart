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

package org.ensembl.mart.lib.config;

import gnu.getopt.Getopt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.LoggingUtils;

/**
 * Application allowing users to dump a Naive DatasetConfig
 * for a given Dataset housed within a given Mart Compliant
 * Database hosted on a given RDMBS host.  
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */

public class NaiveDatasetConfigGenerator {

  private static String dbName = null;
  private static String dbSchema = null;
  private static String dsName = null;
  private static String dbHost = null;
  private static String dbPort = null;
  private static String dbDriver = null;
  private static String dbType = null;
  private static String dbUser = null;
  private static String dbPass = null;
  private static String dsvFileName = null;
  private static String regFileName = null;
  private static boolean printRegistry = false;
  private static boolean verbose = false;
  private static DatasetConfigXMLUtils dscutils = null;
  private static DatabaseDatasetConfigUtils dbutils = null;

  private static final String COMMAND_LINE_SWITCHES = "hvH:U:p:P:T:D:M:d:O:R:";
  private static Logger logger = Logger.getLogger(NaiveDatasetConfigGenerator.class.getName());

  private static String usage() {
    return "NaiveDatasetConfigGenerator <OPTIONS>"
      + "\n"
      + "\n-h                             print this message and exit"
      + "\n-v                             turns on verbose debuggin output"
      + "\n-H                             RDBMS Host (required)"
      + "\n-U                             RDBMS User (required)"
      + "\n-P                             RDBMS Password"
      + "\n-p                             RDBMS Port (defaults to "
      + DetailedDataSource.DEFAULTPORT
      + ")"
      + "\n-T                             RDBMS Type (eg, mysql, oracle:thin, etc. defaults to "
      + DetailedDataSource.DEFAULTDATABASETYPE
      + ")"
      + "\n-D                             RDBMS Driver Name (eg. for class loader, defaults to "
      + DetailedDataSource.DEFAULTDRIVER
      + ")"
      + "\n-M                             Mart Database Name (required)"
      + "\n-d                             DatasetName for requested Naive DatasetConfig (if not provided, a list of potential 'best guess' dataset names will be printed, each with a list of main tables for this dataset for verification purposes)"
      + "\n-O                             Output File Path (defaults to stdout)"
      + "\n-R                             Registry FileName (If specified, creates a MartRegistry Document pointing to file specified in -o switch (ignored if no -o switch available))"
      + "\n";
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

  public static void main(String[] oargs) throws IOException, SQLException, ClassNotFoundException {
    boolean help = false;

    try {
      String[] args = null;
      if (oargs.length > 0) {
        try {
          args = harvestArguments(oargs);
        } catch (Exception e1) {
          System.err.println(e1.getMessage());
          e1.printStackTrace();
          System.exit(1);
        }

        Getopt g = new Getopt(NaiveDatasetConfigGenerator.class.getName(), args, COMMAND_LINE_SWITCHES);
        int c;

        while ((c = g.getopt()) != -1) {

          switch (c) {

            case 'h' :
              help = true;
              break;

            case 'v' :
              verbose = true;
              break;

            case 'H' :
              dbHost = g.getOptarg();
              break;

            case 'U' :
              dbUser = g.getOptarg();
              break;

            case 'P' :
              dbPass = g.getOptarg();
              break;

            case 'p' :
              dbPort = g.getOptarg();
              break;

            case 'T' :
              dbType = g.getOptarg();
              break;

            case 'D' :
              dbDriver = g.getOptarg();
              break;

            case 'M' :
              dbName = g.getOptarg();
              break;

            case 'd' :
              dsName = g.getOptarg();
              break;

            case 'O' :
              dsvFileName = g.getOptarg();
              break;

            case 'R' :
              regFileName = g.getOptarg();
              printRegistry = true;
              break;

            default :
              help = true;
              break;
          }
        }
      } else {
        help = true;
      }

      //check for help
      if (help) {
        System.out.println(usage());
        return;
      }

      if (dbHost == null || dbUser == null || dbName == null) {
        System.out.println("Must set a host (-H), user (-U), and databaseName (-M)\n");
        System.out.println(usage());
        System.exit(1);
      }

      LoggingUtils.setVerbose(verbose);

      dscutils = new DatasetConfigXMLUtils(true);
      
      long start = System.currentTimeMillis();

      if (dbType == null)
        dbType = DetailedDataSource.DEFAULTDATABASETYPE;

      if (dbPort == null)
        dbPort = DetailedDataSource.DEFAULTPORT;

      if (dbDriver == null)
        dbDriver = DetailedDataSource.DEFAULTDRIVER;

      DetailedDataSource dsource =
        new DetailedDataSource(
          dbType,
          dbHost,
          dbPort,
          dbName,
		  dbSchema,
          dbUser,
          dbPass,
          DetailedDataSource.DEFAULTPOOLSIZE,
          dbDriver, "Naive");

      dbutils = new DatabaseDatasetConfigUtils(dscutils, dsource, true);
      
      if (dsName != null) {
        OutputStream dsvOutput = null;

        File dsvFile = null;
        if (dsvFileName != null) {
          dsvFile = new File(dsvFileName);
          dsvOutput = new FileOutputStream(dsvFile);
        } else {
          dsvOutput = System.out;
        }

        DatasetConfig dsv = dbutils.getNaiveDatasetConfigFor(dbName, dsName);

        dscutils.writeDatasetConfigToOutputStream(dsv, dsvOutput);

        if (dsvFileName != null)
          dsvOutput.close();

        if (printRegistry) {
          if (dsvFileName != null) {
            if (regFileName != null) {
              OutputStream regOut = new FileOutputStream(regFileName);
              URLDSConfigAdaptor dsvadaptor = new URLDSConfigAdaptor(dsvFile.toURL(), false, true);
              RegistryDSConfigAdaptor regadaptor = new RegistryDSConfigAdaptor(dsvadaptor);
              MartRegistryXMLUtils.MartRegistryToOutputStream(regadaptor.getMartRegistry(), regOut);
              regOut.close();
            } else {
              System.err.println("Could not print MartRegistry File, no Registry Filename specified");
            }
          } else {
            System.err.println(
              "\nWARNING:Can not print MartRegistry File for DatasetConfig sent to STDOUT. Run again sending the DatasetConfig to a file");
          }
        }
      } else {
        String[] potDsvs = dbutils.getNaiveDatasetNamesFor(dbName);
        System.out.println("Potential Datasets:");
        for (int i = 0, n = potDsvs.length; i < n; i++) {
          String potDS = potDsvs[i];
          System.out.println("\t" + potDS);
          String[] mainTables = dbutils.getNaiveMainTablesFor(dbName, potDS);
          for (int j = 0, m = mainTables.length; j < m; j++) {
            String mainTable = mainTables[j];
            System.out.println("\t\t" + mainTable);
          }
        }
      }

      if (logger.isLoggable(Level.INFO)) {
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        logger.info("\n");
        logger.info("elapsed time (ms) = " + elapsed);
      }

      return;
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1); //exit with error
    }
  }
}
