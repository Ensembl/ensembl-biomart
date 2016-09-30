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
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.util.FormattedSequencePrintStream;
import org.ensembl.util.SequenceUtil;

/**
 * Outputs peptide sequence in one of the supported output format
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see FormatSpec for supported output formats
 */
public final class DownStreamUTRSeqQueryRunner extends BaseSeqQueryRunner {

  private TreeMap locations = new TreeMap();
  private SequenceLocation calcLocation;
  private Hashtable headerinfo = new Hashtable();

  //message to write when no UTR is available 
  private final String noUTRmessage = "No UTR is annotated for this transcript";
  private Logger logger = Logger.getLogger(DownStreamUTRSeqQueryRunner.class.getName());
  
  /**
   * Constructs a PeptideSeqQueryRunner object to execute a Query
   * and print Peptide Sequences
   * 
   * @param query a Query Object
   * @param format a FormatSpec object
   * @param os an OutputStream object
   */
  public DownStreamUTRSeqQueryRunner(Query query, FormatSpec format, OutputStream os) {
    super(query);
    this.format = format;
    this.osr = new FormattedSequencePrintStream(maxColumnLen, os, true); // autoflush true

    switch (format.getFormat()) {
      case FormatSpec.TABULATED :
        this.separator = format.getSeparator();
        this.seqWriter = tabulatedWriter;
        break;

      case FormatSpec.FASTA :
        this.separator = "|";
        this.seqWriter = fastaWriter;
        break;
    }
  }

  protected void updateQuery() {
        Attribute[] exportable = query.getSequenceDescription().getFinalLink();
        
        queryID = exportable[0].getField();
        qualifiedQueryID = exportable[0].getTableConstraint()+"."+queryID;
        chrField = exportable[1].getField();
        coordStart = exportable[2].getField();
        coordEnd = exportable[3].getField();
        strandField = exportable[4].getField();
        rankField = exportable[5].getField();
  }

  protected void processResultSet(Connection conn, ResultSet rs) throws IOException, SQLException {
    if (queryIDindex < 0) {
      ResultSetMetaData rmeta = rs.getMetaData();
      
      // process columnNames for required attribute indices
      for (int i = 1, nColumns = rmeta.getColumnCount(); i <= nColumns; ++i) {
        String column = rmeta.getColumnName(i);
        if (column.equals(queryID) && queryIDindex < 0)
          queryIDindex = i;
        else if (column.equals(rankField) && rankIndex < 0)
          rankIndex = i;
        else if (column.equals(coordStart) && startIndex < 0)
          startIndex = i;
        else if (column.equals(coordEnd) && endIndex < 0)
          endIndex = i;
        else if (column.equals(chrField) && chromIndex < 0)
          chromIndex = i;
        else if (column.equals(strandField) && strandIndex < 0)
          strandIndex = i;
        else
          otherIndices.add(new Integer(i));
       }
    }
		while (rs.next()) {
			Integer keyID = new Integer(rs.getInt(queryIDindex));
			Integer rank = new Integer(rs.getInt(rankIndex));

			if ( keyID.intValue() != lastID ) {
				if ( lastID > -1  ) {
					//This is not the first ID in a batch, process the previous ID sequences
					seqWriter.writeSequences(new Integer(lastID), conn);
				}
				lastIDRowsProcessed = 0; // refresh for the new ID
								
                locations = new TreeMap();
                headerinfo = new Hashtable();
                calcLocation = null;
			}

			int start = rs.getInt(startIndex);
			if (start > 0) {
				// if start is not null, create a new SequenceLocation object from the chr, start, end, and strand
				String chr = rs.getString(chromIndex);
				int end = rs.getInt(endIndex);
				int strand = rs.getInt(strandIndex);

				//	order the locations by their rank in ascending order
				locations.put(rank, new SequenceLocation(chr, start, end, strand));

				// keep track of the lowest start and highest end for the gene	
				if (calcLocation == null) {
					calcLocation = new SequenceLocation(chr, start, end, strand);
				} else {
					if (start < calcLocation.getStart())
						calcLocation = new SequenceLocation(chr, start, calcLocation.getEnd(), strand);
				    if (end > calcLocation.getEnd())
						calcLocation = new SequenceLocation(chr, calcLocation.getStart(), end, strand);
				}
			}

            // Rest can be duplicates, or novel values for a given field, collect lists of values for each field
			// currindex is now the last index of the DisplayIDs.  Increment it, and iterate over the rest of the ResultSet to print the description

			for (int i = 0, n = otherIndices.size(); i < n; i++) {
				int currindex = ((Integer) otherIndices.get(i)).intValue();
				if (rs.getString(currindex) != null) {
					String field = attributes[currindex - 1].getField();
					if (!fields.contains(field))
						fields.add(field);
					String value = rs.getString(currindex);

					if (headerinfo.containsKey(field)) {
						if (!((ArrayList) headerinfo.get(field)).contains(value))
							 ((ArrayList) headerinfo.get(field)).add(value);
					} else {
						List values = new ArrayList();
						values.add(value);
                        headerinfo.put(field, values);
					}
				}
			}
				
			totalRows++;
            totalRowsThisExecute++;
 			resultSetRowsProcessed++;
			lastID = keyID.intValue();
			lastIDRowsProcessed++;
		}
  }

  private final SeqWriter tabulatedWriter = new SeqWriter() {
    void writeSequences(Integer geneID, Connection conn) throws SequenceException {
        
        try {
            if (locations.isEmpty()) {
                for (int j = 0, n = fields.size(); j < n; j++) {
                    if (j > 0)
                        osr.print(separator);
                    String field = (String) fields.get(j);
                    if (headerinfo.containsKey(field)) {
                        List values = (ArrayList) headerinfo.get(field);
                        
                        for (int vi = 0; vi < values.size(); vi++) {
                            if (vi > 0)
                                osr.print(",");
                            osr.print((String) values.get(vi));
                        }
                    }
                }
                osr.print(separator);
                osr.print(noUTRmessage);
                osr.print("\n");
                if (osr.checkError())
                    throw new IOException();
            } else {
                for (int j = 0, n = fields.size(); j < n; j++) {
                    if (j > 0)
                        osr.print(separator);
                    String field = (String) fields.get(j);
                    if (headerinfo.containsKey(field)) {
                        List values = (ArrayList) headerinfo.get(field);
                        
                        for (int vi = 0; vi < values.size(); vi++) {
                            if (vi > 0)
                                osr.print(",");
                            osr.print((String) values.get(vi));
                        }
                    }
                }
                
                osr.print(separator);
                if (osr.checkError())
                    throw new IOException();
                
                //calculate utr flank
                Integer lowRank = (Integer) locations.firstKey();
                Integer highRank = (Integer) locations.lastKey();
                SequenceLocation first_loc = (SequenceLocation) locations.get(lowRank);
                SequenceLocation last_loc = (SequenceLocation) locations.get(highRank);
                
                if (query.getSequenceDescription().getLeftFlank() > 0) {
                    if (first_loc.getStrand() < 0) {
                        int start = calcLocation.getEnd() + 1;
                        int end = start + query.getSequenceDescription().getLeftFlank() - 1;                            
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start, 
                                end,
                                calcLocation.getStrand());
                    } else {
                        int end = calcLocation.getStart() - 1;
                        int start = calcLocation.getStart() - query.getSequenceDescription().getLeftFlank() + 1;
                        if (start < 1)
                            start = 1;
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start,
                                end,
                                calcLocation.getStrand());
                    }
                    
                    //prepend to sequence
                    Integer newLow = new Integer(lowRank.intValue() - 1);
                    locations.put(newLow, calcLocation);
                } else if (query.getSequenceDescription().getRightFlank() > 0) {
                    if (first_loc.getStrand() < 0) {
                        int end = calcLocation.getStart() - 1;
                        int start = end - query.getSequenceDescription().getRightFlank() + 1;                            
                        calcLocation = new SequenceLocation(calcLocation.getChr(),
                                start,
                                end,
                                calcLocation.getStrand());
                    } else {
                        int start = calcLocation.getEnd() + 1;
                        int end = start + query.getSequenceDescription().getRightFlank() - 1;
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start, 
                                end, 
                                calcLocation.getStrand());
                    }
                    
                    //append to sequence
                    Integer newHigh = new Integer(highRank.intValue() + 1);
                    locations.put(newHigh, calcLocation);
                }
                
                for (Iterator lociter = locations.keySet().iterator(); lociter.hasNext();) {
                    SequenceLocation loc = (SequenceLocation) locations.get((Integer) lociter.next());
                    
                    if (loc.getStrand() < 0)
                        osr.write(SequenceUtil.reverseComplement(dna.getSequence(loc.getChr(), loc.getStart(), loc.getEnd())));
                    else
                        osr.write(dna.getSequence(loc.getChr(), loc.getStart(), loc.getEnd()));
                }
                
                osr.print("\n");
                if (osr.checkError())
                    throw new IOException();        
            }
        } catch (SequenceException e) {
            if (logger.isLoggable(Level.WARNING))
                logger.warning(e.getMessage());
            throw e;
        } catch (IOException e) {
            if (logger.isLoggable(Level.WARNING))
                logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
            throw new SequenceException(e);
        }  
    }
  };

  private final SeqWriter fastaWriter = new SeqWriter() {
    void writeSequences(Integer geneID, Connection conn) throws SequenceException {

        try {
            if (locations.isEmpty()) {
                osr.print(">");
                for (int j = 0, n = fields.size(); j < n; j++) {
                    if (j > 0)
                        osr.print(separator);
                    String field = (String) fields.get(j);
                    if (headerinfo.containsKey(field)) {
                        List values = (ArrayList) headerinfo.get(field);
                        
                        for (int vi = 0; vi < values.size(); vi++) {
                            if (vi > 0)
                                osr.print(",");
                            osr.print((String) values.get(vi));
                        }
                    }
                }
                osr.print("\n");
                osr.print(noUTRmessage);
                osr.print("\n");
                if (osr.checkError())
                    throw new IOException();
            } else {
                osr.print(">");
                for (int j = 0, n = fields.size(); j < n; j++) {
                    if (j > 0)
                        osr.print(separator);
                    String field = (String) fields.get(j);
                    if (headerinfo.containsKey(field)) {
                        List values = (ArrayList) headerinfo.get(field);
                        
                        for (int vi = 0; vi < values.size(); vi++) {
                            if (vi > 0)
                                osr.print(",");
                            osr.print((String) values.get(vi));
                        }
                    }
                }
                
                osr.print("\n");
                if (osr.checkError())
                    throw new IOException();
                
                //calculate utr flank
                Integer lowRank = (Integer) locations.firstKey();
                Integer highRank = (Integer) locations.lastKey();
                SequenceLocation first_loc = (SequenceLocation) locations.get(lowRank);
                SequenceLocation last_loc = (SequenceLocation) locations.get(highRank);
                
                if (query.getSequenceDescription().getLeftFlank() > 0) {
                    if (first_loc.getStrand() < 0) {
                        int start = calcLocation.getEnd() + 1;
                        int end = start + query.getSequenceDescription().getLeftFlank() - 1;                            
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start, 
                                end,
                                calcLocation.getStrand());
                    } else {
                        int end = calcLocation.getStart() - 1;
                        int start = calcLocation.getStart() - query.getSequenceDescription().getLeftFlank() + 1;
                        if (start < 1)
                            start = 1;
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start,
                                end,
                                calcLocation.getStrand());
                    }
                    
                    //prepend to sequence
                    Integer newLow = new Integer(lowRank.intValue() - 1);
                    locations.put(newLow, calcLocation);
                } else if (query.getSequenceDescription().getRightFlank() > 0) {
                    if (first_loc.getStrand() < 0) {
                        int end = calcLocation.getStart() - 1;
                        int start = end - query.getSequenceDescription().getRightFlank() + 1;                            
                        calcLocation = new SequenceLocation(calcLocation.getChr(),
                                start,
                                end,
                                calcLocation.getStrand());
                    } else {
                        int start = calcLocation.getEnd() + 1;
                        int end = start + query.getSequenceDescription().getRightFlank() - 1;
                        calcLocation = new SequenceLocation(calcLocation.getChr(), 
                                start, 
                                end, 
                                calcLocation.getStrand());
                    }
                    
                    //append to sequence
                    Integer newHigh = new Integer(highRank.intValue() + 1);
                    locations.put(newHigh, calcLocation);
                }
                
                for (Iterator lociter = locations.keySet().iterator(); lociter.hasNext();) {
                    SequenceLocation loc = (SequenceLocation) locations.get((Integer) lociter.next());
                    
                    if (loc.getStrand() < 0)
                        osr.write(SequenceUtil.reverseComplement(dna.getSequence(loc.getChr(), loc.getStart(), loc.getEnd())));
                    else
                        osr.write(dna.getSequence(loc.getChr(), loc.getStart(), loc.getEnd()));
                }
                
                osr.print("\n");
                if (osr.checkError())
                    throw new IOException();         
            }
        } catch (SequenceException e) {
            if (logger.isLoggable(Level.WARNING))
                logger.warning(e.getMessage());
            throw e;
        } catch (IOException e) {
            if (logger.isLoggable(Level.WARNING))
                logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
            throw new SequenceException(e);
        } 
    }
  };
}
