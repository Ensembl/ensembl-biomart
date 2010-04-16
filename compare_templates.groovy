
def compareTemplates(def source, def target) {
	def sourceConf = new XmlSlurper().parse(source)
	def targetConf = new XmlSlurper().parse(target)
	compareNodes sourceConf, targetConf, "AttributePage"
	compareNodes sourceConf, targetConf, "FilterPage"
}

def compareNodes(def src, def target, def childName) {
	def filters = ["new_attributes","new_filters"]
	//println src.name +"/"+src.attributes.internalName+" <-> "+src.name +"/"+src.attributes.internalName+" -> "+childName
	if(!filters.contains(src.attributes.internalName)) {
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
				def targetChild = targetChildren.get(it.key)
				def srcChild = it.value
				if(targetChild) {
					compareNodes srcChild,targetChild, grandChild 
				} else {
					println "DELETED: "+childName+"/"+it.key
				}
			}
			targetChildren.each { 
				if(!srcChildren.get(it.key)) {
					println "NEW: "+childName+"/"+it.key				
				}
			}
		}
	}
}

def compareNodeByAttributes(def srcNode, def targetNode) {
	srcNode.attributes.each {
		def targVal = targetNode.attributes.get(it.key)		
		if(targVal != it.value) {
			println "DIFF: "+srcNode.name+"/"+srcNode.attributes.internalName+": "+it.value+" <=> "+targVal
		}
	}
}

def hashByName(def conf, def name) {
	def hash = [:]	            
	conf.findAll {
		it.children.findAll { it.name == name }.each {			
			hash.(it.attributes.internalName) = it 
		}
	}
	return hash;
}

def cli = new CliBuilder(usage: 'compare_templates.groovy -source src_xml -target target_xml')
cli.with {
h longOpt: 'help', 'Show usage information'
s longOpt: 'source', args: 1, argName: 'source', 'File containing source template'
t longOpt: 'target', args: 1, argName: 'target', 'File containing target template'
}
 
def options = cli.parse(args)
if (!options) {
System.exit 1
}
if (options.h) {
cli.usage()
System.exit 0
}

compareTemplates options.s,options.t

//"/home/dstaines/biomart/ensembl_mart/e55_template.xml","/home/dstaines/biomart/ensembl_mart/e57_template.xml"