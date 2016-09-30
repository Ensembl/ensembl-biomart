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

package org.ensembl.mart.lib.test;

import java.util.logging.Logger;

import org.ensembl.mart.lib.DetailedDataSource;

/**
 * @author craig
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class DetailedDataSourceTest extends Base {

	private Logger logger =
		Logger.getLogger(DetailedDataSourceTest.class.getName());

	/**
	 * Constructor for DatabaseUtilTest.
	 * @param arg0
	 */
	public DetailedDataSourceTest(String arg0) {
		super(arg0);
	}

	public void testConnectionStringMethod() throws Exception {
		DetailedDataSource ds =
			new DetailedDataSource(databaseType, host, port, databaseName, schema,user, password, 10, jdbcDriver);

		assertTrue("Failed to get any metadata from database", super.connected(ds));
	}
}
