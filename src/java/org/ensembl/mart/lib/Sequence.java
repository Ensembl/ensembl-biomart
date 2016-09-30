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

/**
 * General Object for holding a FASTA representation of a Sequence.
 * The FASTA representation chosen conforms to that used by
 * the various open-bio projects:>displayID\sdescription\nsequence.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */
public class Sequence {
	 private final String fasta = ">";
	 private final String displayID;
	 private final String description;
	 private final String sequence;
	 
	 
	 public Sequence(String displayID, String description, String sequence) {
	 	this.displayID = displayID;
	 	this.description = description;
	 	this.sequence = sequence;
	 }
	 
	public String getDisplayID() {
    	return displayID; 
    }
    
    
    /**
     * Returns the description unformatted.
     * 
	 * @return String description
	 */
	public String getDescription(){
    	return description;
    }
    
     
	/**
	 * Returns the sequence unformmated.
	 * 
	 * @return String sequence
	 */
	public String getSequence() {
		return sequence;
	}
	
    /**
     * returns a FASTA representation of the header:
     * >displayID\sdescription
     * 
	 * @return String header
	 */
	public String getHeader() {
        return (fasta+displayID+" "+description);	
    }
     
    /** 
     * Returns a FASTA record:
     * >displayID\sdescription\nsequence
     * 
     * @return String FASTA Record
	 */
	public String toString() {
        return fasta+displayID+" "+description+"\n"+sequence;
    }
}
