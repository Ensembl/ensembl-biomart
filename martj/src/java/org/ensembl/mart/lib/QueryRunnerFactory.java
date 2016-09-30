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

import java.io.OutputStream;

/**
 * Factory class for generating QueryRunner implimenting objects 
 * based upon the specified Query and FormatSpec.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */
public class QueryRunnerFactory {
    
    /**
     *  Creates a QueryRunner implimenting object for a given Query and 
     *  FormatSpec.
     *  Query.ATTRIBUTE + FormatSpec.TABULATED -> TabulatedQueryRunner
     *
     *  @param Query q
     *  @param FormatSpec f
     *  @throws FormatException
     * @throws InvalidQueryException
     *  @see Query
     *  @see FormatSpec
     */
    public static QueryRunner getInstance(Query q, FormatSpec f, OutputStream out) throws FormatException, InvalidQueryException {
        QueryRunner thisQueryRunner = null;
        switch (q.getType()) {
        
        case Query.ATTRIBUTE:
            if (f.getFormat() == FormatSpec.FASTA)
                throw new FormatException("Fasta format can only be applied to Sequence output");            
            
        thisQueryRunner = new AttributeQueryRunner(q,f, out);
        break;
        
        case Query.SEQUENCE:
            if (q.getSequenceDescription().getSeqType().matches("coding")) {
                thisQueryRunner = new CodingSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("peptide")) {
                thisQueryRunner = new PeptideSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("cdna")) {
                thisQueryRunner = new CdnaSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("transcript_exon")) {
                thisQueryRunner = new TranscriptExonSeqQueryRunner(q,f, out);
            } else if (q.getSequenceDescription().getSeqType().matches("transcript_exon_intron")) {
                thisQueryRunner = new TranscriptEISeqQueryRunner(q,f, out);
            } else if (q.getSequenceDescription().getSeqType().matches("\\w*transcript_flank")) {
                thisQueryRunner = new TranscriptFlankSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("gene_exon_intron")) {
                thisQueryRunner = new GeneEISeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("gene_exon")) {
                thisQueryRunner = new GeneExonSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("\\w*gene_flank")) {
                thisQueryRunner = new GeneFlankSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("3utr")) {
                thisQueryRunner = new DownStreamUTRSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("5utr")) {
                thisQueryRunner = new UpStreamUTRSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("snp")) { 
                thisQueryRunner = new SNPSeqQueryRunner(q,f,out);
            } else if (q.getSequenceDescription().getSeqType().matches("uniprot\\w+"))
                thisQueryRunner = new UniprotSeqQueryRunner(q,f,out);
            else {
                //TODO: impliment java ClassLoader system to pull in client SeqQueryRunner object
                throw new FormatException("Unsuported Query Type " + q.getSequenceDescription().getSeqType() + "\n");							  
            }
        break;
        
        default:
            //TODO: impliment java ClassLoader system to pull in client QueryRunner object
            throw new FormatException("Unsuported Query Type\n");
        }
        return thisQueryRunner;
    }
}
