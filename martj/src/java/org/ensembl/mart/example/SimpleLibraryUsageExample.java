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

package org.ensembl.mart.example;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.BooleanFilter;
import org.ensembl.mart.lib.Engine;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.FormatException;
import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.InputSourceUtil;
import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.LoggingUtils;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.SequenceException;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.RegistryDSConfigAdaptor;

/**
 * Demonstrates how to construct a Query and execute it against a database.
 */
public class SimpleLibraryUsageExample {

    private final static String DEFAULT_REGISTRY_URL = "data/defaultMartRegistry.xml";
    
	/**
	 * Builds a query and executes it against a database.
	 * @param args ignored
	 * @throws SequenceException
	 * @throws FormatException
	 * @throws InvalidQueryException
	 * @throws SQLException
	 * @throws ConfigurationException
	 */
	public static void main(String[] args)
		throws
			SequenceException,
			FormatException,
			InvalidQueryException,
			SQLException,
			ConfigurationException {

		// Configure the logging system, don't show verbose messages
		LoggingUtils.setVerbose(false);

	    URL confURL = null;
        try {
            confURL = InputSourceUtil.getURLForString(DEFAULT_REGISTRY_URL);
        } catch (MalformedURLException e) {
            throw new ConfigurationException("Warning, could not load " + DEFAULT_REGISTRY_URL + " file\n");
        }

	    RegistryDSConfigAdaptor adaptor = new RegistryDSConfigAdaptor(confURL, false, false, true);
		
		// Initialise an engine encapsualting a specific Mart database.
		Engine engine = new Engine();

		// Create a Query object.
		Query query = new Query();
    
        DatasetConfig config = adaptor.getDatasetConfigByDatasetInternalName("hsapiens_gene_ensembl", "default");
        
		query.setDataSource(config.getAdaptor().getDataSource());

		// dataset query applies to
		query.setDataset(config.getDataset());

		// prefixes for databases we want to use
		query.setMainTables(config.getStarBases());

		// primary keys available for sql table joins 
		query.setPrimaryKeys(config.getPrimaryKeys());

		// Attributes to return
        AttributeDescription adesc = config.getAttributeDescriptionByInternalName("gene_stable_id");
    
		query.addAttribute(new FieldAttribute(adesc.getField(), adesc.getTableConstraint(), adesc.getKey()));

		adesc = config.getAttributeDescriptionByInternalName("chr_name");
		query.addAttribute(new FieldAttribute(adesc.getField(), adesc.getTableConstraint(), adesc.getKey()));
		
		adesc = config.getAttributeDescriptionByInternalName("mouse_ensembl_id");
		query.addAttribute(new FieldAttribute(adesc.getField(), adesc.getTableConstraint(), adesc.getKey()));
		
		adesc = config.getAttributeDescriptionByInternalName("mouse_dn_ds");
		query.addAttribute(new FieldAttribute(adesc.getField(), adesc.getTableConstraint(), adesc.getKey()));
		
		String name = "chr_name";    
		FilterDescription fdesc = config.getFilterDescriptionByInternalName(name);
		
		//note, the config system actually masks alot of complexity with regard to filters by requiring the internalName
		//again when calling the getXXX methods
		query.addFilter(new BasicFilter(fdesc.getField(name), fdesc.getTableConstraint(name), fdesc.getKey(name), "=", "22"));
		
		name = "mmusculus_homolog";
		fdesc = config.getFilterDescriptionByInternalName(name);
		
		//note there are different types of BooleanFilter
		if (fdesc.getType(name).equals("boolean"))
		    query.addFilter(new BooleanFilter(fdesc.getField(name), fdesc.getTableConstraint(name), fdesc.getKey(name), BooleanFilter.isNotNULL));
		else
		    query.addFilter(new BooleanFilter(fdesc.getField(name), fdesc.getTableConstraint(name), fdesc.getKey(name), BooleanFilter.isNotNULL_NUM));
		
		
		//Execute the Query and print the results to stdout.
		engine.execute(
		        query,
		        new FormatSpec(FormatSpec.TABULATED, "\t"),
		        System.out);
	}
}
