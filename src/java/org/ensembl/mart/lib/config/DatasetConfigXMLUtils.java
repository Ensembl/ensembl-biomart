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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.jdom.Attribute;
import org.jdom.DocType;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;
import org.xml.sax.InputSource;

/**
 * Utility class containing all necessary XML parsing logic for converting
 * between XML and Object.  Uses JDOM as its XML parsing engine.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class DatasetConfigXMLUtils {

  private Logger logger = Logger.getLogger(DatasetConfigXMLUtils.class.getName());

  //this is the only digest algorithm we support
  public static String DEFAULTDIGESTALGORITHM = "MD5";

  // element names
  private final String DATASETCONFIG = "DatasetConfig";
  private final String STARBASE = "MainTable";
  private final String PRIMARYKEY = "Key";
  private final String BATCHSIZE = "BatchSize";
  private final String IMPORTABLE = "Importable";
  private final String EXPORTABLE = "Exportable";
  private final String FILTERPAGE = "FilterPage";
  private final String FILTERGROUP = "FilterGroup";
  private final String DSFILTERGROUP = "DSFilterGroup";
  private final String FILTERCOLLECTION = "FilterCollection";
  private final String FILTERDESCRIPTION = "FilterDescription";
  private final String ATTRIBUTEPAGE = "AttributePage";
  private final String ATTRIBUTEGROUP = "AttributeGroup";
  private final String ATTRIBUTECOLLECTION = "AttributeCollection";
  private final String ATTRIBUTEDESCRIPTION = "AttributeDescription";
  private final String ATTRIBUTELIST = "AttributeList";
  private final String SPECIFICOPTIONCONTENT = "SpecificOptionContent";
  private final String SPECIFICFILTERCONTENT = "SpecificFilterContent";
  private final String SPECIFICATTRIBUTECONTENT = "SpecificAttributeContent";
  private final String DYNAMICDATASET = "DynamicDataset";
  private final String DSATTRIBUTEGROUP = "DSAttributeGroup";
  private final String OPTION = "Option";
  private final String PUSHACTION = "PushAction";

  // attribute names needed by code
  private final String INTERNALNAME = "internalName";
  private final String OPTPARAM = "optional_parameters";
  private final String SOFTWAREVERSION = "softwareVersion";
  private final String NOCOUNT = "noCount";
  private final String PRIMARYKEYRESTRICTION = "primaryKeyRestriction";
  private final String TEMPLATE = "template";
  private final String VISIBLEFILTERPAGEPARAM = "visibleFilterPage";
  private final String DEFAULTDATASET = "defaultDataset";
  private final String HIDDEN = "hidden";

  private boolean loadFully = false;
  protected boolean includeHiddenMembers = false;

  public DatasetConfigXMLUtils(boolean includeHiddenMembers) {
    this.includeHiddenMembers = includeHiddenMembers;
  }

  /**
   * Set the load behavior of the getDatasetConfigXXX methods. If set to true, all DatasetConfig objects are
   * fully loaded, if false, this is deferred to the lazyLoad system. This is primarily for the DatasetConfigCache
   * object. 
   * @param loadFully -- boolean, if true instructs all subsequent getDatasetConfigXXX calls to fully load the DatasetConfig
   * object before loading it, if false defers this to the lazyLoad system
   */
  protected void setFullyLoadMode(boolean loadFully) {
    this.loadFully = loadFully;
  }

  public DatasetConfig getDatasetConfigForByteArray(byte[] b) throws ConfigurationException {
    return getDatasetConfigForByteArray(b, null);
  }

  /**
   * Returns a DatasetConfig from an XML stored as a byte[], allowing the system to specify whether to
   * load all Elements, or defer this to the lazyLoad system. Also allows system
   * to supply a md5sum digest byte[] array to store into the resulting DatasetConfig.
   *  
   * @param b - byte[] holding XML
   * @param digest -- byte[] containing the digest
   * @return DatasetConfig for xml in byte[]
   * @throws ConfigurationException
   */
  public DatasetConfig getDatasetConfigForByteArray(byte[] b, byte[] digest) throws ConfigurationException {
    ByteArrayInputStream bin = new ByteArrayInputStream(b);
    return getDatasetConfigForXMLStream(bin, digest);
  }

  public DatasetConfig getDatasetConfigForXMLStream(InputStream xmlinput) throws ConfigurationException {
    return getDatasetConfigForXMLStream(xmlinput, null);
  }

  /**
   * Takes an InputStream containing XML, and creates a DatasetConfig object.
   * If a MessageDigest is supplied, this will be added to the DatasetConfig object
   * before returning it.
   *  
   * @param xmlinput -- InputStream containing DatasetConfig.dtd compliant XML
   * @param digest -- byte[] containing the digest
   * @return DatasetConfig
   * @throws ConfigurationException for all underlying Exceptions
   * @see java.security.MessageDigest
   */
  public DatasetConfig getDatasetConfigForXMLStream(InputStream xmlinput, byte[] digest) throws ConfigurationException {
    return getDatasetConfigForDocument(getDocumentForXMLStream(xmlinput), digest);
  }

  /**
   * Takes an InputStream containing DatasetConfig.dtd compliant XML, and creates a JDOM Document.
   * @param xmlinput -- InputStream containin DatasetConfig.dtd compliant XML
   * @return org.jdom.Document
   * @throws ConfigurationException for all underlying Exceptions
   */
  public Document getDocumentForXMLStream(InputStream xmlinput) throws ConfigurationException {
    try {
      SAXBuilder builder = new SAXBuilder();
      // set the EntityResolver to a mart DB aware version, allowing it to get the DTD from the Classpath.
      builder.setEntityResolver(new ClasspathDTDEntityResolver());
      builder.setValidation(false);

      InputSource is = new InputSource(xmlinput);

      Document doc = builder.build(is);

      return doc;
    } catch (Exception e) {
      throw new ConfigurationException(e);
    }
  }

  /**
   * Takes a org.jdom.Document Object representing a DatasetConfig.dtd compliant
   * XML document, and returns a DatasetConfig object.
   * @param doc -- Document representing a DatasetConfig.dtd compliant XML document
   * @return DatasetConfig object
   * @throws ConfigurationException for non compliant Objects, and all underlying Exceptions.
   */
  public DatasetConfig getDatasetConfigForDocument(Document doc) throws ConfigurationException {
    return getDatasetConfigForDocument(doc, null);
  }

  /**
   * Takes a org.jdom.Document Object representing a DatasetConfig.dtd compliant
   * XML document, and returns a DatasetConfig object.  If a MD5SUM Message Digest is
   * supplied, this is added to the DatasetConfig.
   * @param doc -- Document representing a DatasetConfig.dtd compliant XML document
   * @param digest -- a digest computed with the given digestAlgorithm
   * @return DatasetConfig object
   * @throws ConfigurationException for non compliant Objects, and all underlying Exceptions.
   */
  public DatasetConfig getDatasetConfigForDocument(Document doc, byte[] digest) throws ConfigurationException {
    Element thisElement = doc.getRootElement();

    DatasetConfig d = new DatasetConfig();
    loadAttributesFromElement(thisElement, d);

    if (loadFully)
      loadDatasetConfigWithDocument(d, doc);

    if (digest != null)
      d.setMessageDigest(digest);

    return d;
  }

  private void loadAttributesFromElement(Element thisElement, BaseConfigurationObject obj) {
    List attributes = thisElement.getAttributes();

    for (int i = 0, n = attributes.size(); i < n; i++) {
      Attribute att = (Attribute) attributes.get(i);
      String name = att.getName();

      obj.setAttribute(name, thisElement.getAttributeValue(name));
    }
  }

  /**
   * Takes a reference to a DatasetConfig, and a JDOM Document, and parses the JDOM document to add all of the information
   * from the XML for a particular DatasetConfig object into the existing DatasetConfig reference passed into the method.
   * @param dsv -- DatasetConfig reference to be updated
   * @param doc -- Document containing DatasetConfig.dtd compliant XML for dsv
   * @throws ConfigurationException when the internalName returned by the JDOM Document does not match
   *         that of the dsv reference, and for any other underlying Exception
   */
  public void loadDatasetConfigWithDocument(DatasetConfig dsv, Document doc) throws ConfigurationException {
	  Element thisElement = doc.getRootElement();
	  /*
    String intName = thisElement.getAttributeValue(INTERNALNAME, "");
    String optParam = thisElement.getAttributeValue(OPTPARAM, "");
	String softwareVersion = thisElement.getAttributeValue(SOFTWAREVERSION, "");
	String noCount = thisElement.getAttributeValue(NOCOUNT, "");
	String primaryKeyRestriction = thisElement.getAttributeValue(PRIMARYKEYRESTRICTION, "");
	String template = thisElement.getAttributeValue(TEMPLATE, "");
	String defParam = thisElement.getAttributeValue(DEFAULTDATASET, "");
	String visibleFilterPageParam = thisElement.getAttributeValue(VISIBLEFILTERPAGEPARAM, "");
    
    String displayName = thisElement.getAttributeValue("displayName","");
    String description = thisElement.getAttributeValue("description","");
	String version = thisElement.getAttributeValue("version","");
    
    String entryLabel = thisElement.getAttributeValue("entryLabel","");
    
    if (displayName.length() > 0)
    	dsv.setDisplayName(displayName);
    if (description.length() > 0)
    	dsv.setDescription(description);
	if (version.length() > 0)
		dsv.setVersion(version);    	
    	
    
    if (visibleFilterPageParam.length() > 0)
    	dsv.setVisibleFilterPage(visibleFilterPageParam);
    
    if (optParam.length() > 0)
      dsv.setOptionalParameter(optParam);
    
    if (entryLabel.length() > 0)
    	dsv.setEntryLabel(entryLabel);
    
      
	if (softwareVersion.length() > 0)
	  dsv.setSoftwareVersion(softwareVersion);  
	  
	if (noCount.length() > 0)
	  dsv.setNoCount(noCount);    
      
	if (primaryKeyRestriction.length() > 0)
	   dsv.setPrimaryKeyRestriction(primaryKeyRestriction);  
    
    if (template.length() > 0)
	   dsv.setTemplate(template);  
      
	if (defParam.length() > 0)
	  dsv.setDefaultDataset(defParam); 
	  */
	       

    // a DatasetConfig object must have been constructed with an internalName
    // test that the internalNames match , throw an exception if they are not
    
       
    // internalName of document doesn't have to match now
    //if (!intName.equals(dsv.getInternalName()))
    //  throw new ConfigurationException("Document internalName does not match input dsv reference internalName, they may not represent the same data\n");

		loadAttributesFromElement(thisElement, dsv);
	  
    for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
      Element option = (Element) iter.next();
      if (!(Boolean.valueOf(option.getAttributeValue(HIDDEN)).booleanValue()))
        dsv.addOption(getOption(option));
    }


    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, STARBASE));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      dsv.addMainTable(element.getTextNormalize());
    }

    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, PRIMARYKEY));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      dsv.addPrimaryKey(element.getTextNormalize());
    }
	
	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, BATCHSIZE));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  dsv.addBatchSize(element.getTextNormalize());
	}

	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, DYNAMICDATASET));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  dsv.addDynamicDataset(getDynamicDataset(element));
	}
	
	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, IMPORTABLE));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  dsv.addImportable(getImportable(element));
	}
	
	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, EXPORTABLE));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  dsv.addExportable(getExportable(element));
	}

    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, FILTERPAGE));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      dsv.addFilterPage(getFilterPage(element));
    }

    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, ATTRIBUTEPAGE));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      dsv.addAttributePage(getAttributePage(element));
    }

    // we need to manually set the "parent" references on these options
    // so they are availbe for future use.
    List fds = dsv.getAllFilterDescriptions();
    for (Iterator iter = fds.iterator(); iter.hasNext();) {
      FilterDescription fd = (FilterDescription) iter.next();
      fd.setParentsForAllPushOptionOptions(dsv);
    }
  }


  private Importable getImportable(Element thisElement) throws ConfigurationException {
	Importable im = new Importable();
	loadAttributesFromElement(thisElement, im);
		
	return im;
  }
  
  private Exportable getExportable(Element thisElement) throws ConfigurationException {
	Exportable ex = new Exportable();
	loadAttributesFromElement(thisElement, ex);
		
	return ex;
  }

  private FilterPage getFilterPage(Element thisElement) throws ConfigurationException {
    FilterPage fp = new FilterPage();
    loadAttributesFromElement(thisElement, fp);

    for (Iterator iter = thisElement.getDescendants(new MartFilterGroupFilter(includeHiddenMembers)); iter.hasNext();) {
      Element element = (Element) iter.next();
      if (element.getName().equals(FILTERGROUP))
        fp.addFilterGroup(getFilterGroup(element));
      
    }

    return fp;
  }

  private FilterGroup getFilterGroup(Element thisElement) throws ConfigurationException {
    FilterGroup fg = new FilterGroup();
    loadAttributesFromElement(thisElement, fg);

    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, FILTERCOLLECTION));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      fg.addFilterCollection(getFilterCollection(element));
    }

    return fg;
  }



  private FilterCollection getFilterCollection(Element thisElement) throws ConfigurationException {
    FilterCollection fc = new FilterCollection();
    loadAttributesFromElement(thisElement, fc);

    for (Iterator iter = thisElement.getDescendants(new MartFilterDescriptionFilter(includeHiddenMembers));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      fc.addFilterDescription(getFilterDescription(element));
    }

    return fc;
  }

  private Option getOption(Element thisElement) throws ConfigurationException {
    Option o = new Option();
    loadAttributesFromElement(thisElement, o);

    for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
      Element suboption = (Element) iter.next();

	  if (includeHiddenMembers){
		Option o2 = getOption(suboption);
		o2.setParent(o);
		o.addOption(o2);
	  }
	  else if (!(Boolean.valueOf(suboption.getAttributeValue(HIDDEN)).booleanValue())) {
        Option o2 = getOption(suboption);
        o2.setParent(o);
        o.addOption(o2);
      }
    }

	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, SPECIFICOPTIONCONTENT));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  o.addSpecificOptionContent(getSpecificOptionContent(element));
	}

    for (Iterator iter = thisElement.getChildren(PUSHACTION).iterator(); iter.hasNext();) {
      o.addPushAction(getPushOptions((Element) iter.next()));
    }

    return o;
  }

  private PushAction getPushOptions(Element thisElement) throws ConfigurationException {
    PushAction pa = new PushAction();
    loadAttributesFromElement(thisElement, pa);

    for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
      Element option = (Element) iter.next();
	  if (includeHiddenMembers){
		pa.addOption(getOption(option));
	  }
      else if (!(Boolean.valueOf(option.getAttributeValue(HIDDEN)).booleanValue()))
        pa.addOption(getOption(option));
    }

    return pa;
  }
  
  private FilterDescription getFilterDescription(Element thisElement) throws ConfigurationException {
    FilterDescription f = new FilterDescription();
    loadAttributesFromElement(thisElement, f);

	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, SPECIFICFILTERCONTENT));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  f.addSpecificFilterContent(getSpecificFilterContent(element));
	}
		
    for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
      Element option = (Element) iter.next();
      if (includeHiddenMembers){
		Option o = getOption(option);
		o.setParent(f);
		f.addOption(o);
      }
	  else if (!(Boolean.valueOf(option.getAttributeValue(HIDDEN)).booleanValue())) {      
        Option o = getOption(option);
        o.setParent(f);
        f.addOption(o);
      }
    }
    return f;
  }

  private AttributePage getAttributePage(Element thisElement) throws ConfigurationException {
    AttributePage ap = new AttributePage();
    loadAttributesFromElement(thisElement, ap);
    
    for (Iterator iter = thisElement.getDescendants(new MartAttributeGroupFilter()); iter.hasNext();) {
      Element element = (Element) iter.next();
      if (element.getName().equals(ATTRIBUTEGROUP))
        ap.addAttributeGroup(getAttributeGroup(element));
      
    }

    return ap;
  }

  private AttributeGroup getAttributeGroup(Element thisElement) throws ConfigurationException {
    AttributeGroup ag = new AttributeGroup();
    loadAttributesFromElement(thisElement, ag);
    
    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, ATTRIBUTECOLLECTION));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      ag.addAttributeCollection(getAttributeCollection(element));
    }

    return ag;
  }



  private AttributeCollection getAttributeCollection(Element thisElement) throws ConfigurationException {
    AttributeCollection ac = new AttributeCollection();
    loadAttributesFromElement(thisElement, ac);
    
    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, ATTRIBUTEDESCRIPTION));
      iter.hasNext();
      ) {
      Element element = (Element) iter.next();
      ac.addAttributeDescription(getAttributeDescription(element));
    }
    for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, ATTRIBUTELIST));
    iter.hasNext();
    ) {
    Element element = (Element) iter.next();
    ac.addAttributeList(getAttributeList(element));
  }

    return ac;
  }
  
  private AttributeList getAttributeList(Element thisElement) throws ConfigurationException {
	    AttributeList a = new AttributeList();
	    loadAttributesFromElement(thisElement, a);
	    
	    return a;
	  }

  private AttributeDescription getAttributeDescription(Element thisElement) throws ConfigurationException {
    AttributeDescription a = new AttributeDescription();
    loadAttributesFromElement(thisElement, a);

	for (Iterator iter = thisElement.getDescendants(new MartElementFilter(includeHiddenMembers, SPECIFICATTRIBUTECONTENT));
	  iter.hasNext();
	  ) {
	  Element element = (Element) iter.next();
	  a.addSpecificAttributeContent(getSpecificAttributeContent(element));
	}
        
    return a;
  }
  
  private SpecificAttributeContent getSpecificAttributeContent(Element thisElement) throws ConfigurationException {
	  SpecificAttributeContent f = new SpecificAttributeContent();
	 loadAttributesFromElement(thisElement, f);
	 	 
	 return f;	 
  }
  
  private SpecificFilterContent getSpecificFilterContent(Element thisElement) throws ConfigurationException {
	  SpecificFilterContent f = new SpecificFilterContent();
	 loadAttributesFromElement(thisElement, f);
	 
	for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
	  Element option = (Element) iter.next();
	  if (includeHiddenMembers){
		Option o = getOption(option);
		//o.setParent(f);
		o.setParent(null);
		f.addOption(o);
	  }
	  else if (!(Boolean.valueOf(option.getAttributeValue(HIDDEN)).booleanValue())) {      
		Option o = getOption(option);
		//o.setParent(f);
		o.setParent(null);
		f.addOption(o);
	  }
	}
	 
	 return f;	 
  }

  
  private SpecificOptionContent getSpecificOptionContent(Element thisElement) throws ConfigurationException {
	  SpecificOptionContent f = new SpecificOptionContent();
	 loadAttributesFromElement(thisElement, f);
	 
	for (Iterator iter = thisElement.getChildren(OPTION).iterator(); iter.hasNext();) {
	  Element option = (Element) iter.next();
	  if (includeHiddenMembers){
		Option o = getOption(option);
		//o.setParent(f);
		o.setParent(null);
		f.addOption(o);
	  }
	  else if (!(Boolean.valueOf(option.getAttributeValue(HIDDEN)).booleanValue())) {      
		Option o = getOption(option);
		//o.setParent(f);
		o.setParent(null);
		f.addOption(o);
	  }
	}
	 
	 return f;	 
  }
  
  private DynamicDataset getDynamicDataset(Element thisElement) throws ConfigurationException {
	 DynamicDataset a = new DynamicDataset();
	 loadAttributesFromElement(thisElement, a);
	 return a;	 
  }
  
  /**
   * Writes a DatasetConfig object as XML to the given File.  Handles opening and closing of the OutputStream.
   * @param dsv -- DatasetConfig object
   * @param file -- File to write XML
   * @throws ConfigurationException for underlying Exceptions
   */
  public void writeDatasetConfigToFile(DatasetConfig dsv, File file) throws ConfigurationException {
    writeDocumentToFile(getDocumentForDatasetConfig(dsv), file);
  }

  /**
   * Writes a DatasetConfig object as XML to the given OutputStream.  Does not close the OutputStream after writing.
   * If you wish to write a Document to a File, use DatasetConfigToFile instead, as it handles opening and closing the OutputStream.
   * @param dsv -- DatasetConfig object to write as XML
   * @param out -- OutputStream to write, not closed after writing
   * @throws ConfigurationException for underlying Exceptions
   */
  public void writeDatasetConfigToOutputStream(DatasetConfig dsv, OutputStream out) throws ConfigurationException {
    writeDocumentToOutputStream(getDocumentForDatasetConfig(dsv), out);
  }

  /**
   * Writes a JDOM Document as XML to a given File.  Handles opening and closing of the OutputStream.
   * @param doc -- Document representing a DatasetConfig.dtd compliant XML document
   * @param file -- File to write.
   * @throws ConfigurationException for underlying Exceptions.
   */
  public void writeDocumentToFile(Document doc, File file) throws ConfigurationException {
    try {
      FileOutputStream out = new FileOutputStream(file);
      writeDocumentToOutputStream(doc, out);
      out.close();
    } catch (FileNotFoundException e) {
      throw new ConfigurationException(
        "Caught FileNotFoundException writing Document to File provided " + e.getMessage(),
        e);
    } catch (ConfigurationException e) {
      throw e;
    } catch (IOException e) {
      throw new ConfigurationException("Caught IOException creating FileOutputStream " + e.getMessage(), e);
    }
  }

  /**
   * Takes a JDOM Document and writes it as DatasetConfig.dtd compliant XML to a given OutputStream.
   * Does NOT close the OutputStream after writing.  If you wish to write a Document to a File,
   * use DocumentToFile instead, as it handles opening and closing the OutputStream. 
   * @param doc -- Document representing a DatasetConfig.dtd compliant XML document
   * @param out -- OutputStream to write to, not closed after writing
   * @throws ConfigurationException for underlying IOException
   */
  public void writeDocumentToOutputStream(Document doc, OutputStream out) throws ConfigurationException {
    XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());

    try {
      xout.output(doc, out);
    } catch (IOException e) {
      throw new ConfigurationException("Caught IOException writing XML to OutputStream " + e.getMessage(), e);
    }
  }

  private void loadElementAttributesFromObject(BaseConfigurationObject obj, Element thisElement) {
    String[] titles = obj.getXmlAttributeTitles();
	//String[] titles = (String[])obj.attributes.keySet().toArray(new String[0]);

    //sort the attribute titles before writing them out, so that MD5SUM is supported
    Arrays.sort(titles);

    for (int i = 0, n = titles.length; i < n; i++) {
      String key = titles[i];

      if (validString(obj.getAttribute(key)))
        thisElement.setAttribute(key, obj.getAttribute(key));
    }
  }

  /**
   * Takes a DatasetConfig object, and returns a JDOM Document representing the
   * XML for this Object. Does not store DataSource or Digest information
   * @param dsconfig -- DatasetConfig object to be converted into a JDOM Document
   * @return Document object
   */
  public Document getDocumentForDatasetConfig(DatasetConfig dsconfig) {
    Element root = new Element(DATASETCONFIG);
    loadElementAttributesFromObject(dsconfig, root);

	List ads = dsconfig.getDynamicDatasets();
	for (Iterator iter = ads.iterator(); iter.hasNext();)
	   root.addContent(getDynamicDatasetElement((DynamicDataset) iter.next()));

    Option[] os = dsconfig.getOptions();
    for (int i = 0, n = os.length; i < n; i++)
      root.addContent(getOptionElement(os[i]));

    String[] starbases = dsconfig.getStarBases();
    for (int i = 0, n = starbases.length; i < n; i++)
      root.addContent(getStarBaseElement(starbases[i]));

    String[] pkeys = dsconfig.getPrimaryKeys();
    for (int i = 0, n = pkeys.length; i < n; i++)
      root.addContent(getPrimaryKeyElement(pkeys[i]));

	String[] batchSizes = dsconfig.getBatchSizes();
	for (int i = 0, n = batchSizes.length; i < n; i++)
	  root.addContent(getBatchSizeElement(batchSizes[i]));
	

	Importable[] imps = dsconfig.getImportables();
	for (int i = 0, n = imps.length; i < n; i++)
	  root.addContent(getImportableElement(imps[i]));
	  
	Exportable[] exps = dsconfig.getExportables();
	for (int i = 0, n = exps.length; i < n; i++)
	  root.addContent(getExportableElement(exps[i]));
	  

    FilterPage[] fpages = dsconfig.getFilterPages();
    for (int i = 0, n = fpages.length; i < n; i++)
      root.addContent(getFilterPageElement(fpages[i]));

    AttributePage[] apages = dsconfig.getAttributePages();
    for (int i = 0, n = apages.length; i < n; i++)
      root.addContent(getAttributePageElement(apages[i]));

    Document thisDoc = new Document(root);
    thisDoc.setDocType(new DocType(DATASETCONFIG));

    return thisDoc;
  }

  private Element getAttributePageElement(AttributePage apage) {
    Element page = new Element(ATTRIBUTEPAGE);
    loadElementAttributesFromObject(apage, page);

    List groups = apage.getAttributeGroups();
    for (Iterator iter = groups.iterator(); iter.hasNext();) {
      Object group = iter.next();
      if (group instanceof AttributeGroup)
        page.addContent(getAttributeGroupElement((AttributeGroup) group));
      
    }

    return page;
  }



  private Element getAttributeGroupElement(AttributeGroup group) {
    Element ag = new Element(ATTRIBUTEGROUP);
    loadElementAttributesFromObject(group, ag);

    AttributeCollection[] acs = group.getAttributeCollections();
    for (int i = 0, n = acs.length; i < n; i++)
      ag.addContent(getAttributeCollectionElement(acs[i]));

    return ag;
  }

  private Element getAttributeCollectionElement(AttributeCollection collection) {
    Element ac = new Element(ATTRIBUTECOLLECTION);
    loadElementAttributesFromObject(collection, ac);

    List ads = collection.getAttributeDescriptions();
    for (Iterator iter = ads.iterator(); iter.hasNext();)
      ac.addContent(getAttributeDescriptionElement((AttributeDescription) iter.next()));
    ads = collection.getAttributeLists();
    for (Iterator iter = ads.iterator(); iter.hasNext();)
      ac.addContent(getAttributeListElement((AttributeList) iter.next()));

    return ac;
  }
  
  private Element getAttributeListElement(AttributeList attribute) {
	    Element att = new Element(ATTRIBUTELIST);
	    loadElementAttributesFromObject(attribute, att);
	    return att;
	  }

  private Element getAttributeDescriptionElement(AttributeDescription attribute) {
    Element att = new Element(ATTRIBUTEDESCRIPTION);
    loadElementAttributesFromObject(attribute, att);

    for (Iterator i = attribute.getSpecificAttributeContents().iterator(); i.hasNext(); ) 
    	att.addContent(getSpecificAttributeContentElement((SpecificAttributeContent)i.next()));

    return att;
  }


  private Element getSpecificAttributeContentElement(SpecificAttributeContent dynAttribute) {
	 Element datt = new Element(SPECIFICATTRIBUTECONTENT);
	 loadElementAttributesFromObject(dynAttribute, datt);
	 return datt;
  }

  private Element getSpecificFilterContentElement(SpecificFilterContent dynAttribute) {
	 Element datt = new Element(SPECIFICFILTERCONTENT);
	 loadElementAttributesFromObject(dynAttribute, datt);
	 Option[] subops = dynAttribute.getOptions();
	 for (int i = 0, n = subops.length; i < n; i++){
	    datt.addContent(getOptionElement(subops[i]));
	 }
	 return datt;
  }

  private Element getSpecificOptionContentElement(SpecificOptionContent dynAttribute) {
	 Element datt = new Element(SPECIFICOPTIONCONTENT);
	 loadElementAttributesFromObject(dynAttribute, datt);
	 Option[] subops = dynAttribute.getOptions();
	 for (int i = 0, n = subops.length; i < n; i++){
	    datt.addContent(getOptionElement(subops[i]));
	 }
	 return datt;
  }
  
  private Element getDynamicDatasetElement(DynamicDataset dynAttribute) {
	 Element datt = new Element(DYNAMICDATASET);
	 loadElementAttributesFromObject(dynAttribute, datt);
	 return datt;
  }
  
  private Element getFilterPageElement(FilterPage fpage) {
    Element page = new Element(FILTERPAGE);
    loadElementAttributesFromObject(fpage, page);

    List groups = fpage.getFilterGroups();
    for (Iterator iter = groups.iterator(); iter.hasNext();) {
      Object group = iter.next();
      if (group instanceof FilterGroup)
        page.addContent(getFilterGroupElement((FilterGroup) group));
      
    }

    return page;
  }


  /**
   * @param group
   * @return
   */
  private Element getFilterGroupElement(FilterGroup group) {
    Element fg = new Element(FILTERGROUP);
    loadElementAttributesFromObject(group, fg);

    FilterCollection[] acs = group.getFilterCollections();
    for (int i = 0, n = acs.length; i < n; i++)
      fg.addContent(getFilterCollectionElement(acs[i]));

    return fg;
  }

  private Element getFilterCollectionElement(FilterCollection collection) {
    Element fc = new Element(FILTERCOLLECTION);
    loadElementAttributesFromObject(collection, fc);

    List ads = collection.getFilterDescriptions();
    //currently there are only FilterDescription objects, may be DSFilterDescription in the future
    for (Iterator iter = ads.iterator(); iter.hasNext();)
      fc.addContent(getFilterDescriptionElement((FilterDescription) iter.next()));

    return fc;
  }

  private Element getPrimaryKeyElement(String primaryKeyString) {
    Element pkey = new Element(PRIMARYKEY);
    pkey.setText(primaryKeyString);
    return pkey;
  }

  private Element getBatchSizeElement(String batchSizeString) {
	Element bsize = new Element(BATCHSIZE);
	bsize.setText(batchSizeString);
	return bsize;
  }


  private Element getImportableElement(Importable smodule) {
	Element module = new Element(IMPORTABLE);
	loadElementAttributesFromObject(smodule, module);

	return module;
  }
  
  private Element getExportableElement(Exportable smodule) {
	Element module = new Element(EXPORTABLE);
	loadElementAttributesFromObject(smodule, module);
	
	return module;
  }

  private Element getStarBaseElement(String starbaseString) {
    Element sbase = new Element(STARBASE);
    sbase.setText(starbaseString);
    return sbase;
  }



  private Element getOptionElement(Option o) {
    Element option = new Element(OPTION);
    loadElementAttributesFromObject(o, option);

    Option[] subops = o.getOptions();
    for (int i = 0, n = subops.length; i < n; i++)
      option.addContent(getOptionElement(subops[i]));

    PushAction[] pushops = o.getPushActions();
    for (int i = 0, n = pushops.length; i < n; i++)
      option.addContent(getPushActionElement(pushops[i]));

    for (Iterator i = o.getSpecificOptionContents().iterator(); i.hasNext(); ) 
    	option.addContent(getSpecificOptionContentElement((SpecificOptionContent)i.next()));
    
    return option;
  }

  private Element getPushActionElement(PushAction pa) {
    Element pushAction = new Element(PUSHACTION);
    loadElementAttributesFromObject(pa, pushAction);

    Option[] os = pa.getOptions();
    for (int i = 0, n = os.length; i < n; i++)
      pushAction.addContent(getOptionElement(os[i]));

    return pushAction;
  }

  private Element getFilterDescriptionElement(FilterDescription filter) {
    Element fdesc = new Element(FILTERDESCRIPTION);
    loadElementAttributesFromObject(filter, fdesc);

    for (Iterator i = filter.getSpecificFilterContents().iterator(); i.hasNext(); ) 
    	fdesc.addContent(getSpecificFilterContentElement((SpecificFilterContent)i.next()));

    Option[] subops = filter.getOptions();
    for (int i = 0, n = subops.length; i < n; i++)
       fdesc.addContent(getOptionElement(subops[i]));
    
    return fdesc;
  }


  private boolean validString(String test) {
    return (test != null && test.length() > 0);
  }

  /**
   * Given a Document object, converts the given document to an DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM digest using the 
   * JDOM XMLOutputter writing to a java.security.DigestOutputStream.  This is the default method for calculating the MessageDigest 
   * of a DatasetConfig Object used in various places in the MartJ system.
   * @param doc -- Document object representing a DatasetConfig.dtd compliant XML document. 
   * @return byte[] digest algorithm
   * @throws ConfigurationException for NoSuchAlgorithmException, and IOExceptions.
   * @see java.security.DigestOutputStream
   */
  public byte[] getMessageDigestForDocument(Document doc) throws ConfigurationException {
    try {
      MessageDigest mdigest = MessageDigest.getInstance(DEFAULTDIGESTALGORITHM);
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DigestOutputStream dout = new DigestOutputStream(bout, mdigest);
      XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());

      xout.output(doc, dout);

      byte[] digest = mdigest.digest();

      bout.close();
      dout.close();

      System.out.println("DIGEST FOR DOC " + doc.toString() + " IS " + digest.toString());

      return digest;
    } catch (NoSuchAlgorithmException e) {
      throw new ConfigurationException(
        "Digest Algorithm " + DEFAULTDIGESTALGORITHM + " does not exist, possibly a problem with the Java Installation\n",
        e);
    } catch (IOException e) {
      throw new ConfigurationException("Caught IOException converting Docuement to Digest\n", e);
    }
  }

  /**
   * Returns a MessageDigest digest for a DatasetConfig by first creating a JDOM Document object, and then
   * calculuating its digest using DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM. 
   * @param dsv -- A DatasetConfig object
   * @return byte[] digest
   * @throws ConfigurationException for all underlying Exceptions
   */
  public byte[] getMessageDigestForDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    return getMessageDigestForDocument(getDocumentForDatasetConfig(dsv));
  }

  /**
   * This method does not convert the raw bytes of a given InputStream into a Message Digest.  It is intended to calculate a Message Digest
   * that is comparable between multiple XML representations of the same DatasetConfig Object (despite one representation having an Element with
   * an Attribute specified with an empty string, and the other having the same Element with that Attribute specification missing entirely, or each
   * containing the same Element with the same attribute specifications, but occuring in a different order within the XML string defining the Element).  
   * It does this by first converting the InputStream into a DatasetConfig Object (using XMLStreamToDatasetConfig(is)), and then calculating the 
   * digest on the resulting DatasetConfig Object (using DatasetConfigToMessageDigest(dsv, DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM)).
   * @param xmlinput -- InputStream containing DatasetConfig.dtd compliant XML.
   * @return byte[] digest
   * @throws ConfigurationException for all underlying Exceptions
   */
  public byte[] getMessageDigestForXMLStream(InputStream is) throws ConfigurationException {
    return getMessageDigestForDocument(getDocumentForXMLStream(is));
  }

  /**
   * Returns a byte[] of XML for the given DatasetConfig object.
   * @param dsv - DatasetConfig object to be parsed into a byte[]
   * @return byte[] representing XML for DatasetConfig
   * @throws ConfigurationException for underlying exceptions
   */
  public byte[] getByteArrayForDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    writeDatasetConfigToOutputStream(dsv, bout);
    return bout.toByteArray();
  }

}
