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
package org.ensembl.mart.util;

 /**
  * Immutable container for descriptive information about a column
  * in an RDBMS table.  Meant to be held in a TableDescription object.
  * dbType refers to the String returned from the RDBMS that describes the column
  * type in a describe table query.  javaType refers to a java.sql.Types enum. 
  * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
  * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
  * @see java.sql.Types
  */
  public class ColumnDescription {
		public final String name;
		public final String dbType;
		public final int javaType;
		public final int maxLength;
		
		public ColumnDescription(String name, String dbType, int javaType, int maxLength) {
			this.name = name;
			this.dbType = dbType;
			this.javaType = javaType;
			this.maxLength = maxLength;
		}
  }
