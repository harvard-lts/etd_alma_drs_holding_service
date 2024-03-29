from etd.drs_holding_by_dropbox import DRSHoldingByDropbox
import lxml.etree as ET
import os.path


class TestDRSHoldingByDropbox():

    def test_getFromMets(self):
        """
        Test case for the getFromMets method of DRSHoldingByDropbox class.
        """
        metsFile = "./tests/data/samplemets.xml"
        school = "gsd"

        drs_holding = DRSHoldingByDropbox('1234567890',
                                          'URN-3:HUL.DRS.OBJECT:12345678',
                                          None,
                                          True)
        result = drs_holding.getFromMets(metsFile, school)

        assert result is not None
        assert result['school'] == school
        assert 'dateCreated' in result
        assert "title" in result

    def test_writeMarcXml(self):
        """
        Test case for the writeMarcXml method of DRSHoldingByDropbox class.
        """
        batch = "almadrsholding2023071720-993578-gsd"
        drs_holding = DRSHoldingByDropbox('1234567890',
                                          'URN-3:HUL.DRS.OBJECT:12345678',
                                          None,
                                          True)
        batchOutputDir = "./tests/data/in/proquest2023071720-993578-gsd" # noqa

        school = "gsd"
        # Generate the data
        generatedMarcXmlValues = {}
        generatedMarcXmlValues['titleIndicator2'] = '0'
        generatedMarcXmlValues['title'] = "Naming Expeditor: Reimagining Institutional Naming System at Harvard" # noqa
        generatedMarcXmlValues['dateCreated'] = "2023-05"
        generatedMarcXmlValues['school'] = school
        verbose = False

        # Write the marcxml
        drs_holding.writeMarcXml(batch, batchOutputDir,
                                 generatedMarcXmlValues, verbose)
        marcFile = batchOutputDir + "/" + batch + ".xml"
        assert os.path.exists(marcFile)
        namespace_mapping = {'marc': 'http://www.loc.gov/MARC21/slim'}
        doc = ET.parse(marcFile)
        pqidXPath = "//marc:record/marc:datafield[@tag='035']" \
                    "/marc:subfield[@code='a']"
        titleXPath = "//marc:record/marc:datafield[@tag='245']" \
                     "/marc:subfield[@code='a']"
        objecturnXPath = "//marc:record/marc:datafield[@tag='852']" \
                         "/marc:subfield[@code='z']"
        libcodeXPath = "//marc:record/marc:datafield[@tag='909']" \
                       "/marc:subfield[@code='k']"
        assert doc.xpath(pqidXPath, namespaces=namespace_mapping)[0].text == \
            "(ProQuestETD)1234567890"
        assert doc.xpath(titleXPath, namespaces=namespace_mapping)[0].text == \
            "Naming Expeditor: Reimagining Institutional Naming System at Harvard" # noqa
        assert doc.xpath(objecturnXPath, namespaces=namespace_mapping)[0] \
            .text == "Preservation object,URN-3:HUL.DRS.OBJECT:12345678"
        assert doc.xpath(libcodeXPath, namespaces=namespace_mapping)[0] \
            .text == "netdes"
        os.remove(marcFile)
