/**
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

/**
 * Interface for all QueryRunner Objects.  Defines a consistent behavior
 * for objects returned by a QueryRunnerFactory.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see QueryRunnerFactory
 */
public interface QueryRunner {
	/**
	 * execute a Query using a specified mySQL database Connection, and output
	 * the results to a specified OutputStream.  Different implimentations can format the output
	 * as required.
	 * 
	 * @param conn A JDBC Connection
	 * @param os An OutputStream
	 * @param limit An integer limit on the query (adds a limit by clause to the query)
   * @param isSubQuery - SubQueryListFilterHandler passes true, everything else passes false
	 * @throws SQLException
	 * @throws IOException
	 * @throws InvalidQueryException
	 */
  public void execute(int limit, boolean isSubQuery) throws SequenceException , InvalidQueryException;
}
