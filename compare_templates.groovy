import java.util.HashMap;
import java.util.Map;

import groovy.xml.StreamingMarkupBuilder 
import groovy.xml.XmlUtil;


def compareTemplates(def source, def target) {
	def sourceConf = new XmlParser().parse(source)
	def targetConf = new XmlParser().parse(target)
	compareNodes sourceConf, targetConf, "AttributePage"
	compareNodes sourceConf, targetConf, "FilterPage"
	return sourceConf
}

def isBlackListed(def name) {
	def found = false
	["new_attributes","new_filters",".*paralog.*",".*homolog.*",".*ortholog.*","multispecies","new_filters"].each {
		if(name.toString().toLowerCase().matches(it)) {
			found = true
		}
	}           
	return found
}

def compareNodes(def src, def target, def childName) {
	if(!isBlackListed(src.attributes().internalName)) {
		compareNodeByAttributes(src,target)
		if(childName) {
			def relations = [
					"AttributePage":"AttributeGroup",
					"AttributeGroup":"AttributeCollection",
					"AttributeCollection":"AttributeDescription",
					"FilterPage":"FilterGroup",
					"FilterGroup":"FilterCollection",
					"FilterCollection":"FilterDescription",
					"FilterDescription":"Option"
					]
			def grandChild = relations.get(childName)
			def srcChildren = hashByName(src,childName)
			def targetChildren = hashByName(target,childName)
			srcChildren.each { 
				def targetChild = targetChildren.get(it.key.toLowerCase())
				def srcChild = it.value
				if(targetChild) {
					compareNodes srcChild,targetChild, grandChild 
				} else {
					println "DELETED: "+childName+"/"+it.key
				}
			}
			targetChildren.each { 
				if(!srcChildren.get(it.key.toLowerCase()) && !isBlackListed(it.key)) {
					println "NEW: "+it.value.name()+"/"+it.key
					def newNode = new Node(src,
							it.value.name(),
				            it.value.attributes())
//					if(src.children.getClass().name == 'groovy.util.slurpersupport.NodeChildren') {
						src.appendNode(newNode)						
//					} else {
//						src.children.add(it.value)
//					}
				}
			}
		}
	}
}

def compareNodeByAttributes(def srcNode, def targetNode) {
	srcNode.attributes().each {
		def targVal = targetNode.attributes().get(it.key)		
		if(targVal != it.value) {
			println "DIFF: "+srcNode.name()+"/"+srcNode.attributes().internalName+": "+it.value+" <=> "+targVal
			srcNode.attributes().put(it.key,it.value)
		}
	}
}

def hashByName(def conf, def name) {
	def hash = [:]	
	conf.depthFirst().grep{ it.getClass().name == 'groovy.util.Node' && it.name() == name }.each {			
			hash.(it.attributes().internalName.toLowerCase()) = it 
		}
	return hash;
}

def cli = new CliBuilder(usage: 'compare_templates.groovy -source src_xml -target target_xml')
cli.with {
	h longOpt: 'help', 'Show usage information'
	s longOpt: 'source', args: 1, argName: 'source', 'File containing source template'
	t longOpt: 'target', args: 1, argName: 'target', 'File containing target template'
	m longOpt:'merge', 'Merge the changes into a new file'
}

def options = cli.parse(args)
if (!options) {
	System.exit 1
}
if (options.h) {
	cli.usage()
	System.exit 0
}

def merged = compareTemplates(options.s,options.t)
if(options.m) {
	new XmlNodePrinter(new PrintWriter(new FileWriter(options.s+".merged"))).print(merged)
	new XmlNodePrinter(new PrintWriter(new FileWriter(options.s+".original"))).print(new XmlParser().parse(options.s))
}
