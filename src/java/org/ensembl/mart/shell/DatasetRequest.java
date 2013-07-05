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
package org.ensembl.mart.shell;

import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.config.ConfigurationException;

/** 
 * Simply parses a request for dataset
 * into its constituent parts.
 * Either mart.dataset.config, mart.dataset, dataset.config, or dataset
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */
public final class DatasetRequest {
  private final String DATASOURCEDELIMITER = ">";
  private final MartShellLib msl;
  public final String mart;
  public final String dataset;
  public final String datasetconfig;

  public DatasetRequest(String request, MartShellLib msl) throws InvalidQueryException {
    this.msl = msl;
    String datasetreq = request;

    String mrt = null;
    String dset = null;
    String config = null;

    try {
      if (request.indexOf(DATASOURCEDELIMITER) > 0) {
        String[] toks = request.split(DATASOURCEDELIMITER);
        datasetreq = toks[0];
        //mrt = msl.deCanonicalizeMartName( toks[1] ); 
        mrt = toks[1] ; //CHANGED

        	if (!msl.adaptorManager.supportsAdaptor(mrt))
            throw new InvalidQueryException("Mart " + mrt + " has not been added, use add Mart\n");

          if (msl.adaptorManager.getAdaptorByName(mrt).getDataSource() == null)
            throw new InvalidQueryException(
              "Mart " + mrt + " File Mart Sources cannot be used in the name>martName syntax\n");
      }

      String[] toks = datasetreq.split("\\.");

      if (toks.length == 3) {
        //sourcename.datasetname.configname

        //dont use toks[0] as mart if x>y parsed above
        if (mrt == null)
        {  
        	//mrt = msl.deCanonicalizeMartName( toks[0] );
        	mrt = toks[0] ; //CHANGED
        }
        if (!msl.adaptorManager.supportsAdaptor(mrt))
          throw new InvalidQueryException(
            "Sourcename " + mrt + " from datasetconfig request " + request + " is not a known source\n");
            
        if (!msl.adaptorManager.getAdaptorByName(mrt).supportsDataset(toks[1]))
          throw new InvalidQueryException(
            "Dataset "
              + toks[1]
              + " is not supported by sourcename "
              + mrt
              + " in datasetconfig request "
              + request
              + "\n");

        dset = toks[1];
        config = toks[2];
      } else if (toks.length == 2) {
        //either sourcename.datasetname or datasetname.configname relative to envMart
                
        if (msl.adaptorManager.supportsAdaptor(toks[0] /*msl.deCanonicalizeMartName( toks[0] )*/ ) ) //CHANGED
        {
          //assume it is sourcename.datasetname
          
          if (mrt == null)
          { 
        	  //mrt = msl.deCanonicalizeMartName( toks[0] );
        	  mrt = toks[0];	// 	CHANGED
          } 
          if (!msl.adaptorManager.supportsAdaptor(mrt))
            throw new InvalidQueryException(
              "Sourcename " + toks[0] + " from datasetconfig request " + request + " is not a known source\n");

          if (!msl.adaptorManager.getAdaptorByName(mrt).supportsDataset(toks[1]))
            throw new InvalidQueryException(
              "Dataset "
                + toks[1]
                + " is not supported by sourcename "
                + mrt
                + " in datasetconfig request "
                + request
                + "\n");

          dset = toks[1];
          config = MartShellLib.DEFAULTDATASETCONFIGNAME;
        } else {
          //assume it is datasetname.configname relative to envMart or x>y request
          if (mrt == null && msl.envMart == null)
            throw new InvalidQueryException(
              "Must set environmental Mart to manipulate DatasetConfigs with relative name " + request + "\n");

          if (mrt == null &&
            msl.adaptorManager.getAdaptorByName(msl.envMart.getName())
              .getDatasetConfigByDatasetInternalName(toks[0], toks[1]) != null)
            mrt = msl.envMart.getName();
          dset = toks[0];
          config = toks[1];
        }
      } else if (toks.length == 1) {
        if (mrt == null && msl.envMart == null)
          throw new InvalidQueryException(
            "1 - Shazi: DatasetRequest: Must set environmental Mart to manipulate DatasetConfigs with relative name " + request + "\n");

        //either datasetname relative to envMart or configname relative to envMart.envDataset
        if (msl.adaptorManager.supportsDataset(toks[0])) {
          //assume it is datasetname relative to envMart
          
          if (mrt == null)
            mrt = msl.envMart.getName();
          
          dset = toks[0];
          config = MartShellLib.DEFAULTDATASETCONFIGNAME;
        } else {
          //assume it is configname relative to envMart and envDataset, or x>y request relative to envDataset
          if (msl.envDataset == null)
            throw new InvalidQueryException(
              "2- Shazi: DatasetRequest: Must set environmental Dataset to manipulate DatasetConfigs with relative name " + request + "\n");

          if (mrt == null)
            mrt = msl.envMart.getName();
          
          dset = msl.envDataset.getDataset();
          config = toks[0];
        }
      }
    } catch (ConfigurationException e) {
      throw new InvalidQueryException(
        "Caught ConfigurationException manipulating DatasetConfig named " + request + " " + e.getMessage(),
        e);
    } catch (InvalidQueryException e) {
      throw e;
    }

    mart = mrt;
    dataset = dset;
    datasetconfig = config;
  }
}