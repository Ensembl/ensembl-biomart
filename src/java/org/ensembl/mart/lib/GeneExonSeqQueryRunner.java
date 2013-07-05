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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.util.FormattedSequencePrintStream;
import org.ensembl.util.SequenceUtil;

/**
 * Outputs Gene Exon sequences in one of the supported formats
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see FormatSpec for supported output formats
 */
public final class GeneExonSeqQueryRunner extends BaseSeqQueryRunner {

    private List idsSeen = new ArrayList();
	private final String LOCATION = "location";
    private Hashtable exonatts = new Hashtable();
	private Logger logger = Logger.getLogger(GeneExonSeqQueryRunner.class.getName());
	
  /**
   * Constructs a GeneExonSeqQueryRunner object to execute a Query
   * and print Exon Sequences for each Gene
   * 
   * @param query a Query Object
   * @param format a FormatSpec object
   * @param os an OutputStream object
   */
  public GeneExonSeqQueryRunner(Query query, FormatSpec format, OutputStream os) {
    super(query);
    this.format = format;
    this.osr = new FormattedSequencePrintStream(maxColumnLen, os, true); //autoflush true

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
  }

  protected void processResultSet(Connection conn, ResultSet rs) throws IOException, SQLException {
    if (queryIDindex < 0) {
      ResultSetMetaData rmeta = rs.getMetaData();
      
      // process columnNames for required attribute indices
      for (int i = 1, nColumns = rmeta.getColumnCount(); i <= nColumns; ++i) {
        String column = rmeta.getColumnName(i);
        
        if (column.equals(queryID) && queryIDindex < 0)
          queryIDindex = i;
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
      int start = rs.getInt(startIndex);
      
      //skip exons which have been done already, and skip if start not defined
      if (!idsSeen.contains(keyID) && (start > 0)) {
          // if start is not null, create a new SequenceLocation object from the chr, start, end, and strand
          String chr = rs.getString(chromIndex);
          int end = rs.getInt(endIndex);
          int strand = rs.getInt(strandIndex);
          
          exonatts.put(LOCATION, new SequenceLocation(chr, start, end, strand));
          
          // Rest can be duplicates, or novel values for a given field, collect lists of values for each field
          // currindex is now the last index of the DisplayIDs.  Increment it, and iterate over the rest of the ResultSet to print the description
          
          for (int i = 0, n = otherIndices.size(); i < n; i++) {
              int currindex = ((Integer) otherIndices.get(i)).intValue();
              if (rs.getString(currindex) != null) {
                  String field = attributes[currindex - 1].getField();
                  if (!fields.contains(field))
                      fields.add(field);
                  
                  String value = rs.getString(currindex);
                  
                  if (exonatts.containsKey(field)) {
                      if (!((ArrayList) exonatts.get(field)).contains(value))
                          ((ArrayList) exonatts.get(field)).add(value);
                  } else {
                      List values = new ArrayList();
                      values.add(value);
                      exonatts.put(field, values);
                  }
              }
          }
          
          //will only do each exon once
          seqWriter.writeSequences(keyID, conn);
          idsSeen.add(keyID);
          exonatts = new Hashtable();
      }
      
      totalRows++;
      totalRowsThisExecute++;
      resultSetRowsProcessed++;
      lastID = keyID.intValue();
    }
  }

  private final SeqWriter tabulatedWriter = new SeqWriter() {
    void writeSequences(Integer geneID, Connection conn) throws SequenceException {
      if (!idsSeen.contains(geneID)) {
          try {
              SequenceLocation exonloc = (SequenceLocation) exonatts.get(LOCATION);
              
              for (int j = 0, n = fields.size(); j < n; j++) {
                  if (j > 0)
                      osr.print(separator);
                  String field = (String) fields.get(j);
                  if (exonatts.containsKey(field)) {
                      List values = (ArrayList) exonatts.get(field);
                      
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
              
              //extend flanking sequence if necessary
              int lflank = query.getSequenceDescription().getLeftFlank();
              int rflank = query.getSequenceDescription().getRightFlank();
              
              if (lflank > 0)
                  exonloc = exonloc.extendLeftFlank(lflank);
              if (rflank > 0)
                  exonloc = exonloc.extendRightFlank(rflank);
              
              // write out the sequence
              if (exonloc.getStrand() < 0)
                  osr.write(
                          SequenceUtil.reverseComplement(
                                  dna.getSequence(exonloc.getChr(), exonloc.getStart(), exonloc.getEnd())));
              else
                  osr.write(dna.getSequence(exonloc.getChr(), exonloc.getStart(), exonloc.getEnd()));
              
              osr.print("\n");
              
              if (osr.checkError())
                  throw new IOException();
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
    }
  };

  private final SeqWriter fastaWriter = new SeqWriter() {
      void writeSequences(Integer geneID, Connection conn) throws SequenceException {
          if (!idsSeen.contains(geneID)) {
              try {
                  osr.print(">");
                  SequenceLocation exonloc = (SequenceLocation) exonatts.get(LOCATION);
                  
                  for (int j = 0, n = fields.size(); j < n; j++) {
                      if (j > 0)
                          osr.print(separator);
                      
                      String field = (String) fields.get(j);
                      if (exonatts.containsKey(field)) {
                          List values = (ArrayList) exonatts.get(field);
                          
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
                  
                  //extend flanking sequence if necessary
                  int lflank = query.getSequenceDescription().getLeftFlank();
                  int rflank = query.getSequenceDescription().getRightFlank();
                  
                  if (lflank > 0)
                      exonloc = exonloc.extendLeftFlank(lflank);
                  if (rflank > 0)
                      exonloc = exonloc.extendRightFlank(rflank);
                  
                  // write out the sequence
                  if (exonloc.getStrand() < 0)
                      osr.writeSequence(
                              SequenceUtil.reverseComplement(
                                      dna.getSequence(exonloc.getChr(), exonloc.getStart(), exonloc.getEnd())));
                  else
                      osr.writeSequence(dna.getSequence(exonloc.getChr(), exonloc.getStart(), exonloc.getEnd()));
                  
                  osr.print("\n");
                  osr.resetColumnCount();
                  
                  if (osr.checkError())
                      throw new IOException();
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
      }
  };
}
