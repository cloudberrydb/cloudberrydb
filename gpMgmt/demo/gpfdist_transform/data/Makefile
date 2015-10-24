all: dblp rig joost 

dblp: dblp.xml dblp.dtd

dblp.xml: dblp.xml.gz
	gunzip -c $< > $@

dblp.xml.gz:
	wget http://www.cs.washington.edu/research/xmldatasets/data/dblp/dblp.xml.gz

dblp.dtd:
	wget http://www.cs.washington.edu/research/xmldatasets/data/dblp/dblp.dtd

rig: rig.xml

rig.xml: WITSML_Data_Schema_v1311.zip
	unzip -p WITSML_Data_Schema_v1311.zip XML_Examples/rig.xml | dos2unix > rig.xml

WITSML_Data_Schema_v1311.zip:
	wget http://w3.energistics.org/schema/WITSML_Data_Schema_v1311.zip

joost: joost.jar

joost.jar: joost-0.9.1-bin.zip
	unzip -p joost-0.9.1-bin.zip joost-0.9.1/joost.jar > joost.jar

joost-0.9.1-bin.zip:
	wget http://sourceforge.net/projects/joost/files/joost/0.9.1/joost-0.9.1-bin.zip/download

clean:
	-rm -f [^R]*.xml *.jar *.zip *.dtd *.gz
