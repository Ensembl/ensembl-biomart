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

package org.ensembl.mart.util;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Convenience methods related to java.util.logging package
 * configuration.
 */
public class LoggingUtil {
  
  /**
   * @return true if java.util.logging.config.file JVM parameter 
   * provided, else false
   */
  public static boolean isLoggingConfigFileSet() {
    return System.getProperty("java.util.logging.config.file")!=null;
  }
  
  /**
   * Enable ALL logging messages to
   * be written to the default log handlers on the root logger. By default these 
   * handlers logging levels are set to INFO which prevents 
   * CONFIG, FINE, FINER and FINEST messages being output.
   *
   */
  public static void setAllRootHandlerLevelsToFinest() {
    setHandlerLevels(Logger.getLogger(""), Level.FINEST);
  }


  /**
   * Convenience method which sets the logging level for all logging handlers to level.
   * @param level logging level 
   */
  public static void setAllHandlerLevels(Level level) {
    setHandlerLevels(Logger.getLogger(""), level);
  }

  /**
   * Sets the logging level for all logger's handlers.
   * @param logger logger of interest
   * @param level logging level
   */
  public static void setHandlerLevels(Logger logger, Level level) {
    Handler[] handlers = logger.getHandlers();
    for (int index = 0; index < handlers.length; index++) {
      handlers[index].setLevel(level);
    }
  }
}
