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

import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Implimentation of the EntityResolver specifically desinged to handle DOCTYPE declarations
 * differently than the default provided.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class ClasspathDTDEntityResolver implements EntityResolver {

   private final String MARTJARPROTOCAL = "classpath";
   
   private Logger logger = Logger.getLogger(ClasspathDTDEntityResolver.class.getName());
   
   /**
    * Constructs a MartDTDEntityResolver object to add to an XML (SAX, DOM) Parser for MartConfiguration.xml
    * to allow it to pull the DTD from a different source than that specified in the DOCTYPE declaration.
    */
   public ClasspathDTDEntityResolver() {     
   }
   
	/**
   * Implements the resolveEntity method, but overrides systemIDs containing the protocal
   * 'classpath:' to get the entity represented in the path component of the URL from the martj.jar file. 
   * If the systemID does not contain classpath: as the protocal, then it returns
   * a null InputSource, allowing JDOM to locate the requested Entity in its default manner.
   * (eg. if you want the system to fetch DatasetConfig.dtd from the martj.jar use 'classpath:DatasetConfig.dtd', 
   * but if you want it to fetch 'DatasetConfig.dtd' from the file system, or some other URL, 
   * use 'file:DatasetConfig.dtd', 'DatasetConfig.dtd', 
   * or 'http://url_to_DatasetConfig.dtd'). 
   * 
	 * @see org.xml.sax.EntityResolver#resolveEntity(java.lang.String, java.lang.String)
	 */
	public InputSource resolveEntity(String publicID, String systemID) throws SAXException, IOException {
		    
		if (systemID.indexOf(MARTJARPROTOCAL) >= 0) {
      if (logger.isLoggable(Level.INFO))
      logger.fine("Getting DTD " + systemID + " from martj.jar\n");
        
      StringTokenizer tokens = new StringTokenizer(systemID, ":");
      tokens.nextToken();
      String path = tokens.nextToken();
      InputStream is = ClasspathDTDEntityResolver.class.getClassLoader().getResourceAsStream( path );

      if (is == null)
        throw new SAXException("classpath protocal systemID " + systemID + " recieved, but ClassLoader could not find the corresponding dtd in the jar file\n");
          
      return new InputSource(is);           
		}
		else
		  return null;
	}
}
