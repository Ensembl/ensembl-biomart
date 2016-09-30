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
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.util.FormattedSequencePrintStream;
import org.ensembl.util.SequenceUtil;

/**
 * This object prints out Transcripts )Exons and Introns) in one of the supported formats
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see FormatSpec for supported output formats
 */
public final class TranscriptEISeqQueryRunner extends BaseSeqQueryRunner {

  private final String TRANSCRIPTS = "transcripts";
	private final String LOCATION = "location";
  private Logger logger = Logger.getLogger(TranscriptEISeqQueryRunner.class.getName());

  /**
   * Constructs a TranscriptEISeqQueryRunner object to print 
   *  Transcripts (exons and introns), with optional flanking sequences
   * 
   * @param query
   * @param format
   * @param os
   */
  public TranscriptEISeqQueryRunner(Query query, FormatSpec format, OutputStream os) {
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

      // want everything ordered by gene_id, transcript_id
      if (keyID.intValue() != lastID) {
        if (lastID > -1) {
          //This is not the first ID in a batch, process the previous ID sequences
          seqWriter.writeSequences(new Integer(lastID), conn);
        }

        //refresh the iDs TreeMap  
        iDs = new TreeMap();
        lastIDRowsProcessed = 0; // refresh for the new ID

        Hashtable atts = new Hashtable();
        iDs.put(keyID, atts);
      }
      Hashtable tranatts = (Hashtable) iDs.get(keyID);

      int start = rs.getInt(startIndex);
      if (start > 0) {
        // if start is not null, create a new SequenceLocation object from the chr, start, end, and strand
        String chr = rs.getString(chromIndex);
        int end = rs.getInt(endIndex);
        int strand = rs.getInt(strandIndex);

        // keep track of the lowest start and highest end for the transcript
        if (!(tranatts.containsKey(LOCATION)))
          tranatts.put(LOCATION, new SequenceLocation(chr, start, end, strand));
        else {
          SequenceLocation tranloc = (SequenceLocation) tranatts.get(LOCATION);
          if (start < tranloc.getStart())
            tranatts.put(LOCATION, new SequenceLocation(chr, start, tranloc.getEnd(), strand));
          if (end > tranloc.getEnd())
            tranatts.put(LOCATION, new SequenceLocation(chr, tranloc.getStart(), end, strand));
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

          if (tranatts.containsKey(field)) {
            if (!((ArrayList) tranatts.get(field)).contains(value))
               ((ArrayList) tranatts.get(field)).add(value);
          } else {
            ArrayList values = new ArrayList();
            values.add(value);
            tranatts.put(field, values);
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
        Hashtable tranatts = (Hashtable) iDs.get(geneID);
        SequenceLocation tranloc = (SequenceLocation) tranatts.get(LOCATION);

          for (int j = 0, n = fields.size(); j < n; j++) {
            if (j > 0)
              osr.print(separator);
            
            String field = (String) fields.get(j);
            if (tranatts.containsKey(field)) {
              ArrayList values = (ArrayList) tranatts.get(field);

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

          //extend flanks, if necessary, and write sequence
          if (query.getSequenceDescription().getLeftFlank() > 0)
            tranloc = tranloc.extendLeftFlank(query.getSequenceDescription().getLeftFlank());
          if (query.getSequenceDescription().getRightFlank() > 0)
            tranloc = tranloc.extendRightFlank(query.getSequenceDescription().getRightFlank());

          if (tranloc.getStrand() < 0)
            osr.write(
              SequenceUtil.reverseComplement(
                dna.getSequence(tranloc.getChr(), tranloc.getStart(), tranloc.getEnd())));
          else
            osr.write(dna.getSequence(tranloc.getChr(), tranloc.getStart(), tranloc.getEnd()));

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
  };

  private final SeqWriter fastaWriter = new SeqWriter() {
    void writeSequences(Integer geneID, Connection conn) throws SequenceException {
      try {
        Hashtable tranatts = (Hashtable) iDs.get(geneID);
        SequenceLocation tranloc = (SequenceLocation) tranatts.get(LOCATION);
        osr.print(">");

        if (osr.checkError())
          throw new IOException();

        for (int j = 0, n = fields.size(); j < n; j++) {
          if (j > 0)
            osr.print(separator);
          String field = (String) fields.get(j);
          if (tranatts.containsKey(field)) {
            ArrayList values = (ArrayList) tranatts.get(field);

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

        //extend flanks, if necessary, and write sequence
        if (query.getSequenceDescription().getLeftFlank() > 0)
          tranloc = tranloc.extendLeftFlank(query.getSequenceDescription().getLeftFlank());
        if (query.getSequenceDescription().getRightFlank() > 0)
          tranloc = tranloc.extendRightFlank(query.getSequenceDescription().getRightFlank());

        if (tranloc.getStrand() < 0)
          osr.writeSequence(
            SequenceUtil.reverseComplement(
              dna.getSequence(
                      tranloc.getChr(), 
                      tranloc.getStart(), 
                      tranloc.getEnd())));
        else
          osr.writeSequence(
                  dna.getSequence(
                          tranloc.getChr(), 
                          tranloc.getStart(), 
                          tranloc.getEnd()));

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
  };
}
