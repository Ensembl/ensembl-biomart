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

package org.ensembl.mart.lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility package for methods to get URL or InputStream objects from
 * various sources.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class InputSourceUtil {

	private static Logger logger = Logger.getLogger(InputSourceUtil.class.getName());

	/**
	 * Allows client to get a URL for a given URL (any string with a ':') or Path specified as a String.
	 * In either case, the string is converted to a URL (either directly, or via a new File().toURL call).
	 * If a MalformedURLException is encountered, a second attempt is made to return a URL for the source
	 * from the CLASSPATH, stripping off any URL protocal, if present.
	 * @param source -- either a path, or a URL string
	 * @return URL
	 * @throws MalformedURLException if all attempts to parse the source into a URL fail
	 */
	public static URL getURLForString(String source) throws MalformedURLException {
		URL ret = null;

		try {
			if (source.indexOf(":") > 1) {
				//URL
				ret = new URL(source);
        
        if (! new File(ret.getPath()).exists())
          ret = getURLFromClassPath(source);
			} else {
				//file path
				ret = new URL("file:"+source);
			}
		} catch (MalformedURLException e) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Could not load " + source + " as is, trying CLASSPATH");

			ret = getURLFromClassPath(source);
		}

		if (ret == null)
			throw new MalformedURLException("Could not create a URL from the specified source " + source + "\n");

		return ret;
	}

	/**
	 * Allows client to get an InputStream for a given URL (any string with a ':') or Path specified as a String.
	 * If the source is a Path request, and a FileNotFoundException or IOException is encountered for the original source,
	 * a second attempt is made to find and open the file from the CLASSPATH.  If this second attempt fails with
	 * an IOException, the original FileNotFoundException is thrown.  If the source is a URL request, and any URL
	 * related Exception is encountered, a second attempt will be made to open the path portion of the URL from the CLASSPATH.
	 * If any exception is encountered, the original URL related Exception will be thrown.
	 * @param source -- String either URL string, or path
	 * @return InputStream
	 * @throws IOException -- Underlying Exception from attempt to open any InputStream
	 * @throws MalformedURLException -- when the source is interpreted as a URL, but is Malformed
	 * @throws FileNotFoundException --  when the file is not found as specified, or in the CLASSPATH.
	 */
	public static InputStream getStreamForString(String source) throws IOException, MalformedURLException, FileNotFoundException {
		InputStream ret = null;

		try {
			if (source.indexOf(":") > 1) {
				//URL
				URL url = new URL(source);
				ret = url.openStream();
			} else {
				//file path
				FileInputStream f = new FileInputStream(source);
				ret = f;
			}
		} catch (MalformedURLException e) {

			try {
				ret = getStreamFromClassPath(source);
			} catch (IOException e1) {
				// throw the original MalformedURLException
				throw e;
			}
		} catch (FileNotFoundException e) {
			//try finding the file path in the classpath

			try {
				ret = getStreamFromClassPath(source);
			} catch (IOException e1) {
				// throw the original FileNotFoundException
				throw e;
			}
		} catch (IOException e) {
			try {
				ret = getStreamFromClassPath(source);
			} catch (IOException e1) {
				// throw the original IOException
				throw e;
			}
		}

		if (ret == null)
			throw new IOException("Could not create an InputStream from the specified source " + source + "\n");

		return ret;
	}

	/**
	 * Allows client to get an InputStream for a given URL
	 * If any URL related Exception is encountered, a second attempt will be made to open 
	 * the path portion of the URL from the CLASSPATH.
	 * If any exception is encountered during the second attempt, the original URL related Exception will be thrown.
	 * @param source -- URL
	 * @return InputStream from URL, or its Path component as loaded from the CLASSPATH
	 * @throws IOException -- Underlying Exception from attempt to open any InputStream
	 */
	public static InputStream getStreamForURL(URL source) throws IOException {
		InputStream ret = null;

		try {
			ret = source.openStream();
		} catch (IOException e) {
			try {
				ret = getStreamFromClassPath(source.getPath());
			} catch (IOException e1) {
				// throw the original IOException
				throw e;
			}
		}

		if (ret == null)
			throw new IOException("Could not create an InputStream from the specified source " + source + "\n");

		return ret;
	}

	private static InputStream getStreamFromClassPath(String source) throws IOException {
    URL retURL = getURLFromClassPath(source);
    if (retURL == null)
      return null;
      
    return retURL.openStream();
	}

	private static URL getURLFromClassPath(String source) {
		//remove the protocal: from a URL string, if present
		if (source.indexOf(":") >= 0)
			source = source.substring(source.indexOf(":"));

		return ClassLoader.getSystemResource(source);
	}
}
