/*
 * Copyright (C) 2003 EBI, GRL
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 * 
 * Created on Dec 7, 2004
 *
 */

package org.ensembl.mart.util;

import gnu.getopt.Getopt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Hashtable;
import java.util.Iterator;

import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.InputSourceUtil;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.MartRegistry;
import org.ensembl.mart.lib.config.MartRegistryXMLUtils;

/**
 * @author dlondon@ebi.ac.uk
 */
public class MartRegistryDBTool {

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
					throw new Exception("Invalid Arguments Passed to MartRegistryDBTool\n");
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

	private static boolean load = false; //-l
	private static boolean fetch = false; //-f
	private static String registryPath; //-r
	private static String host; //-H
	private static String port; //-P
	private static String type; //-T
	private static String instance; //-I
	private static String schema; //-S
	private static String user; //-U
	private static String password; //-p
	private static String registryName = null; //
	private static boolean compress = true; //-X means do NOT compress
	private static boolean help = false; //-h

	private static String COMMAND_LINE_SWITCHES = "hXl:f:H:P:T:I:S:U:p:";

	private static String usage() {
		return "\nusage: MartRegistryDBTool "
			+ "\n -l registryPath. loads MartRegistry file at registryPath into the Database"
			+ "\n -f registryPath. Fetches MartRegistry in database into registryPath"
			+ "\n  ***  one of -l or -f is required"
			+ "\n -H DB Host. REQUIRED"
			+ "\n -P DB Port. OPTIONAL"
			+ "\n -T Database Type (OPTIONAL, defaults to mysql, also supports oracle:thin)"
			+ "\n -I name of Database Instance to load to or fetch from. REQUIRED"
		    + "\n -S name of schema to load to or fetch from. REQUIRED"
			+ "\n -U DB User REQUIRED"
			+ "\n -p DB Password OPTIONAL"
			+ "\n -X if loading (-l) do not compress the XML in the Database (default is to compress)"
			+ "\n -h print this message\n";
	}

	public static void main(String[] args) {

		String[] nargs = null;
		if (args.length > 0) {
			try {
				nargs = harvestArguments(args);
			} catch (Exception e1) {
				System.err.println(e1.getMessage());
				e1.printStackTrace();
				System.exit(1);
			}

			Getopt g = new Getopt("MartRegistryDBTool", nargs, COMMAND_LINE_SWITCHES);
			int c;

			while ((c = g.getopt()) != -1) {

				switch (c) {

					case 'h' :
						help = true;
						break;

					case 'l' :
						load = true;
						registryPath = g.getOptarg();
						break;

					case 'f' :
						fetch = true;
						registryPath = g.getOptarg();
						break;

					case 'X' :
						compress = false;
						break;

					case 'H' :
						host = g.getOptarg();
						break;

					case 'P' :
						port = g.getOptarg();
						break;

					case 'T' :
						type = g.getOptarg();
						break;

					case 'I' :
						instance = g.getOptarg();
						break;
						
					case 'S' :
						schema = g.getOptarg();
						break;	

					case 'U' :
						user = g.getOptarg();
						break;

					case 'p' :
						password = g.getOptarg();
						break;
				}
			}
		} else {
			System.err.println(usage());
			System.exit(1);
		}

		if (help) {
			System.out.println(usage());
			System.exit(0);
		}

		if (load && fetch) {
			System.err.println("Specify only one of -l or -f, not both\n");
			System.err.println(usage());
			System.exit(1);
		}

		if (!(load || fetch)) {
			System.err.println("Specify one of -l or -f\n");
			System.err.println(usage());
			System.exit(1);
		}

        if (registryPath == null || registryPath.equals("")) {
          System.err.println("No registryPath specified with -l or -f switch.\n");
          System.err.println(usage());
          System.exit(1);
        }
         
        if (host == null) {
          System.err.println("No Host specified with -H switch.\n");
          System.err.println(usage());
          System.exit(1);
        } 
        
        if (instance == null) {
          System.err.println("No instance specified with -I switch.\n");
          System.err.println(usage());
          System.exit(1);
        }
        
		if (schema == null) {
			System.err.println("No schema specified with -S switch.\n");
			System.err.println(usage());
			System.exit(1);
		}
        
        if (user == null) {
          System.err.println("No user specified with -U switch.\n");
          System.err.println(usage());
          System.exit(1);
        } 
        
		String jdbcClass = DetailedDataSource.getJDBCDriverClassNameFor(type);

		// apply defaults only if both dbtype and jdbcdriver are null
		if (type == null && jdbcClass == null) {
			type = DetailedDataSource.DEFAULTDATABASETYPE;
			jdbcClass = DetailedDataSource.DEFAULTDRIVER;
		}

		String connectionString = DetailedDataSource.connectionURL(type, host, port, instance);

		// use default name
     	registryName = connectionString;

		//use the default poolsize of 10        
		DetailedDataSource dsource =
			new DetailedDataSource(
				type,
				host,
				port,
				instance,
				schema,
				user,
				password,
				DetailedDataSource.DEFAULTPOOLSIZE,
				jdbcClass,
				registryName);

		MartRegistry martreg = null;
		if (load) {
			try {
				MartRegistryXMLUtils.storeMartRegistryDocumentToDataSource(
					dsource,
					MartRegistryXMLUtils.XMLStreamToDocument(
					        InputSourceUtil.getStreamForString(registryPath),
					        false
					),
					compress);
			} catch (MalformedURLException e) {
				System.err.println("Recieved invalid URL " + registryPath + ": " + e.getMessage() + "\n");
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				System.err.println("File: " + registryPath + " does not exist\n");
				e.printStackTrace();
			} catch (ConfigurationException e) {
				System.err.println("Could not load Registry " + registryPath + ": " + e.getMessage() + "\n");
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Could not load Registry " + registryPath + ": " + e.getMessage() + "\n");
				e.printStackTrace();
			}
		} else if (fetch) {
			try {
				MartRegistryXMLUtils.DocumentToFile(
				  MartRegistryXMLUtils.DataSourceToRegistryDocument(dsource),
				  new File(registryPath)
				);
			} catch (ConfigurationException e) {
				System.err.println("Could not fetch Registry from " + dsource.getName() + ". Check your connection params\n");
				//e.printStackTrace();
			}
		} // else not needed

		System.err.println("All Complete");
		System.exit(0);
	}
}
