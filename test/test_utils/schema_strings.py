json_schema_string: str = '''
{
    "id": "https://example.com/address.schema.json",
    "schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "An address similar to http://microformats.org/wiki/h-card",
    "type": "object",
    "properties": {
        "data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                "A": {
                "type": "number"
                },
                "B": {
                "type": "number"
                },
                "C": {
                "type": "integer"
                },
                "D": {
                "type": "number"
                },
                "E": {
                "type": "string"
                },
                "F": {
                "type": "string"
                },
                "G": {
                "type": "string",
                "format": "date-time"
                },
                "H": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "format": "uuid"
                        },
                        "value": {
                            "type": "integer"
                        }
                    },
                    "required": ["id", "value"]
                }
            },
            "required": [ "A", "B", "C", "D", "E", "F", "G", "H"]
            }
        }
    },
    "required": ["data"]
}'''

xml_schema_string: str = '''
<xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="data">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="item" maxOccurs="unbounded" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="A">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:float">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="B">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:float">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="C">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:byte">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="D">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:float">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="E">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:string">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="F">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:string">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="G">
                <xs:complexType>
                  <xs:simpleContent>
                    <xs:extension base="xs:dateTime">
                      <xs:attribute type="xs:string" name="type" use="optional"/>
                    </xs:extension>
                  </xs:simpleContent>
                </xs:complexType>
              </xs:element>
              <xs:element name="H">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="id">
                      <xs:complexType>
                        <xs:simpleContent>
                          <xs:extension base="xs:string">
                            <xs:attribute type="xs:string" name="type" use="optional"/>
                          </xs:extension>
                        </xs:simpleContent>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="value">
                      <xs:complexType>
                        <xs:simpleContent>
                          <xs:extension base="xs:byte">
                            <xs:attribute type="xs:string" name="type" use="optional"/>
                          </xs:extension>
                        </xs:simpleContent>
                      </xs:complexType>
                    </xs:element>
                  </xs:sequence>
                  <xs:attribute type="xs:string" name="type" use="optional"/>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
            <xs:attribute type="xs:string" name="type" use="optional"/>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
'''

def _get_schema_string(format: str = "json") -> str:
    format = format.lower()
    mapping: dict[str, str] = {
        "json": json_schema_string,
        "xml": xml_schema_string
    }
    assert format in mapping.keys(), f"format must be either json or xml, received {format}"
    return mapping[format]
    