/*
 Copyright (C) 2006 EBI
 
 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the itmplied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.biomart.common.resources;

import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Simple wrapper for locating file resources within this package, and for
 * reading internationalisation messages from the messages file.
 * <p>
 * The {@link #setResourceLocation(String)} method must be called before this
 * class is used.
 * <p>
 * Note that it will search <tt>org/biomart/common/resources</tt> if it cannot
 * find the requested resource in the location specified using
 * {@link #setResourceLocation(String)}.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.11 $, $Date: 2007-08-21 15:19:55 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class Resources {
	/**
	 * The current version of the BioMart software.
	 */
	public static String BIOMART_VERSION = "0.7";

	private static String location = null;

	private static ResourceBundle bundle = null;

	private final static ResourceBundle commonBundle = ResourceBundle
			.getBundle("org/biomart/common/resources/messages");

	/**
	 * Sets the resource location for the application.
	 * 
	 * @param location
	 *            the resource location, e.g.
	 *            <tt>org/biomart/builder/resources</tt>. The messages file
	 *            <tt>messages.properties</tt> and other resources requested
	 *            from now on will be found in this location.
	 */
	public static void setResourceLocation(final String location) {
		Resources.location = location;
		final String resourcesFileName = location + "/messages";
		Log.info("Loading resources from " + resourcesFileName);
		Resources.bundle = ResourceBundle.getBundle(resourcesFileName);
		Log.info("Done loading resources");
	}

	/**
	 * Obtain a resource value given the key for it.
	 * 
	 * @param key
	 *            the key to lookup.
	 * @return the value found, or <tt>null</tt> if not found.
	 */
	private static String getValue(final String key) {
		String value = null;
		if (Resources.bundle != null)
			try {
				value = Resources.bundle.getString(key);
			} catch (final MissingResourceException e) {
				value = null;
			}
		if (value == null)
			value = Resources.commonBundle.getString(key);
		return value;
	}

	/**
	 * Obtains a string from the messages resource bundle. Runs it through
	 * MessageFormat before returning. See
	 * {@link ResourceBundle#getString(String)} for full description of
	 * behaviour.
	 * 
	 * @param key
	 *            the key to look up.
	 * @return the matching string.
	 */
	public static String get(final String key) {
		return MessageFormat.format(Resources.getValue(key), new Object[] {});
	}

	/**
	 * Obtains a string from the messages resource bundle. Substitutes the first
	 * parameter in the resulting string for the specified value using
	 * MessageFormat. See {@link ResourceBundle#getString(String)} for full
	 * description of behaviour.
	 * 
	 * @param key
	 *            the key to look up.
	 * @param value
	 *            the value to substitute in the first placeholder of the string
	 *            we looked up.
	 * @return the matching string.
	 */
	public static String get(final String key, final String value) {
		return MessageFormat.format(Resources.getValue(key),
				new Object[] { value });
	}

	/**
	 * Obtains a string from the messages resource bundle. Substitutes all
	 * parameters in the resulting string for the specified values using
	 * MessageFormat. See {@link ResourceBundle#getString(String)} for full
	 * description of behaviour.
	 * 
	 * @param key
	 *            the key to look up.
	 * @param values
	 *            the values to substitute in the placeholders in the looked-up
	 *            string. There should be the same number of values as there are
	 *            placeholders.
	 * @return the matching string.
	 */
	public static String get(final String key, final String[] values) {
		return MessageFormat.format(Resources.getValue(key), values);
	}

	/**
	 * Given a resource name (a file inside some package somewhere), return a
	 * stream that will read the contents of that file.
	 * 
	 * @param resource
	 *            the classpath of the resource to lookup, e.g. "myfile.txt".
	 * @return a stream that will read that file.
	 */
	public static InputStream getResourceAsStream(final String resource) {
		final String commonResource = "org/biomart/common/resources/"
				+ resource;
		final String locationResource = Resources.location == null ? commonResource
				: Resources.location + "/" + resource;
		final ClassLoader cl = Resources.class.getClassLoader();
		return (InputStream) AccessController
				.doPrivileged(new PrivilegedAction() {
					public Object run() {
						InputStream resource;
						if (cl != null) {
							resource = cl.getResourceAsStream(locationResource);
							if (resource == null)
								resource = cl
										.getResourceAsStream(commonResource);
						} else {
							resource = ClassLoader
									.getSystemResourceAsStream(locationResource);
							if (resource == null)
								resource = ClassLoader
										.getSystemResourceAsStream(commonResource);
						}
						return resource;
					}
				});
	}

	/**
	 * Given a resource name (a file inside some package somewhere), return a
	 * URL pointing to it.
	 * 
	 * @param resource
	 *            the classpath of the resource to lookup, e.g. "myfile.txt".
	 * @return a URL pointing to that file.
	 */
	public static URL getResourceAsURL(final String resource) {
		final String commonResource = "org/biomart/common/resources/"
				+ resource;
		final String locationResource = Resources.location == null ? commonResource
				: Resources.location + "/" + resource;
		final ClassLoader cl = Resources.class.getClassLoader();
		return (URL) AccessController.doPrivileged(new PrivilegedAction() {
			public Object run() {
				URL resource;
				if (cl != null) {
					resource = cl.getResource(locationResource);
					if (resource == null)
						resource = cl.getResource(commonResource);
				} else {
					resource = ClassLoader.getSystemResource(locationResource);
					if (resource == null)
						resource = ClassLoader
								.getSystemResource(commonResource);
				}
				return resource;
			}
		});
	}

	// Private means that this class is a static singleton.
	private Resources() {
	}
}
