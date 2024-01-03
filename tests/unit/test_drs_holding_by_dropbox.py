from etd.drs_holding_by_dropbox import DRSHoldingByDropbox

class TestDRSHoldingByDropbox():

    def test_getFromMets(self):
        metsFile = "/home/etdadm/tests/data/samplemets.xml"
        school = "gsd"

        drs_holding = DRSHoldingByDropbox('1234567890')
        result = drs_holding.getFromMets(metsFile, school)

        assert result is not None
        assert result['proquestId'] == '1234567890'
        assert result['school'] == school
        assert 'dateCreated' in result
        assert "title" in result

    def test_getFromMets_missing_values(self):
        metsFile = "/home/etdadm/tests/data/samplemets.xml"
        school = "gsd"

        # Simulate missing values in the mets file
        drs_holding = DRSHoldingByDropbox(None)
        
        result = drs_holding.getFromMets(metsFile, school)

        assert not result
    
    def test_writeMarcXml(self, monkeypatch):
        batch = "alma2023071720-993578-gsd"
        drs_holding = DRSHoldingByDropbox('1234567890')
        batchOutputDir = "/home/etdadm/tests/data/in/proquest2023071720-993578-gsd"
        metsFile = os.path.join(batchOutputDir, "mets.xml")
        school = "gsd"
        # Generate the data
        generatedMarcXmlValues = drs_holding.getFromMets(metsFile, school)
        verbose = False

        # Write the marcxml
        drs_holding.writeMarcXml(batch, batchOutputDir, generatedMarcXmlValues, verbose)
        marcFile = batchOutputDir + "/" + batch + ".xml"
        assert os.path.exists(marcFile)
        namespace_mapping = {'marc': 'http://www.loc.gov/MARC21/slim'}
        doc = ET.parse(marcFile)
        authorXPath = "//marc:record/marc:datafield[@tag='100']" \
                      "/marc:subfield[@code='a']"
        degreeXPath = "//marc:record/marc:datafield[@tag='100']" \
                      "/marc:subfield[@code='c']"
        titleXPath = "//marc:record/marc:datafield[@tag='245']" \
                     "/marc:subfield[@code='a']"
        schoolXPath = "//marc:record/marc:datafield[@tag='502']" \
                      "/marc:subfield[@code='a']"
        advisorXpath = "//marc:record/marc:datafield[@tag='720']" \
                       "/marc:subfield[@code='a']"
        abstractXPath = "//marc:record/marc:datafield[@tag='520']" \
                        "/marc:subfield[@code='a']"
        dashXpath = "//marc:record/marc:datafield[@tag='856']" \
                    "/marc:subfield[@code='u']"
        assert doc.xpath(authorXPath,
                         namespaces=namespace_mapping)[0].text == \
            "Peng, Yolanda Yuanlu"
        assert doc.xpath(degreeXPath,
                         namespaces=namespace_mapping)[0].text == \
            "(MDes, Harvard University, 2023)"
        assert doc.xpath(titleXPath, namespaces=namespace_mapping)[0].text == \
            "Naming Expeditor: " \
            "Reimagining Institutional Naming System at Harvard"
        assert doc.xpath(schoolXPath,
                         namespaces=namespace_mapping)[0].text == \
            "Thesis (MDes, Master in Design Studies, " \
            "Department of Urban Planning and Design) -- " \
            "Harvard Graduate School of Design, May 2023."
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[0].text == \
            "Shoshan, Malkit,"
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[1].text == \
            "Bruguera, Tania,"
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[5].text == \
            "Claudio, Yazmin C,"
        assert doc.xpath(abstractXPath,
                         namespaces=namespace_mapping)[0].text == \
            abstractText
        assert doc.xpath(dashXpath,
                         namespaces=namespace_mapping)[0].text == \
            "https://nrs.harvard.edu/urn-3:HUL.InstRepos:993578"
