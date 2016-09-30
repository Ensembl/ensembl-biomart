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

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/** 
 * Utility class providing static methods to modify the behavior of the
 * Java logging system
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
* @see java.util.logging.Logger
*/
public class LoggingUtils {

    /**
     * Provides faciltiy to set or modify the Configuration of the underlying java.util.logging system.
     * @param conf -- InputStream containing java.util.logging properties configuration file.
     * @throws SecurityException
     * @throws IOException
     */
    public static void setLoggingConfiguration(InputStream conf) throws SecurityException, IOException {
			LogManager.getLogManager().readConfiguration( conf );
    }
    
    /**
     * Provides facility to set or modify the output level of the underlying java.util.logging system. Note, this can
     * override any levels explicitly set using a configuration file with setLoggingConfiguraton.  It is best to use this
     * method in the absense of configuration files.
     * @param verbose
     */
    public static void setVerbose(boolean verbose) {
			Level level =  verbose  ?  Level.INFO :  Level.WARNING ;
			Logger rootLogger = Logger.getLogger("");
			rootLogger.setLevel( level );
    }
}
