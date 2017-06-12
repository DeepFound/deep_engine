#include "org/w3c/dom/DomUtil.h"

using namespace org::w3c::dom;

const char* str =
"<schema protocol=\"1\" version=\"v1\">"
	"<table name=\"t1\">"
		"<index_list>"
			"<index name=\"idx1\""
				"length=\"10\""
				"primary=\"1\""
				"unique=\"1\""
				"hidden=\"0\">"

				"<column_list>"
					"<column name=\"c1\""
						"type=\"1\""
						"length=\"8\""
						"length_bytes=\"0\""
						"null_bit=\"0\""
						"null_offset=\"0\""
						"reserved=\"1\" />"

					"<column name=\"c2\""
						"type=\"2\""
						"length=\"64\""
						"length_bytes=\"2\""
						"null_bit=\"1\""
						"null_offset=\"0\""
						"reserved=\"0\" />"

				"</column_list>"
			"</index>"
		"</index_list>"
	"</table>"
"</schema>";

static const char* SCHEMA          = "schema";
static const char* SCHEMA_PROTOCOL = "protocol";
static const char* SCHEMA_VERSION  = "version";

static const char* TABLE           = "table";
static const char* TABLE_NAME      = "name";

static const char* INDEX_LIST      = "index_list";

static const char* INDEX           = "index";
static const char* INDEX_NAME      = "name";
static const char* INDEX_LENGTH    = "length";
static const char* INDEX_PRIMARY   = "primary";
static const char* INDEX_UNIQUE    = "unique";
static const char* INDEX_HIDDEN    = "hidden";

static const char* COLUMN_LIST          = "column_list";
static const char* COLUMN               = "column";
static const char* COLUMN_NAME          = "name";
static const char* COLUMN_LENGTH        = "length";
static const char* COLUMN_LENGTH_BYTES  = "length_bytes";
static const char* COLUMN_NULL_BIT      = "null_bit";
static const char* COLUMN_NULL_OFFSET   = "null_offset";
static const char* COLUMN_RESERVED      = "reserved";

int read() {
	Document* doc = DomUtil::parse(str);

	Element schema;

	doc->getFirstChild(&schema);

	inttype protocol = schema.getAttributeInt(SCHEMA_PROTOCOL);
	const char* version = schema.getAttribute(SCHEMA_VERSION);

	fprintf(stderr, "SCHEMA protocol=%d version=%s\n", protocol, version);

	Element table;
	schema.getFirstChild(&table);

	const char* tableName = table.getAttribute(TABLE_NAME);
	fprintf(stderr, "\tTABLE name=%s\n", tableName);

	Element indexList;
	table.getFirstChild(&indexList);

	Element index;
	indexList.getFirstChild(&index);

	const char* indexName = index.getAttribute(INDEX_NAME);
	inttype indexLength = index.getAttributeInt(INDEX_LENGTH);
	boolean indexPrimary = index.getAttributeBool(INDEX_PRIMARY);
	boolean indexUnique = index.getAttributeBool(INDEX_UNIQUE);
	boolean indexHidden = index.getAttributeBool(INDEX_HIDDEN);

	fprintf(stderr, "\t\tINDEX name=%s length=%d primary=%d unique=%d hidden=%d\n", indexName, indexLength, indexPrimary, indexUnique, indexHidden);

	Element indexColumnList;
	index.getFirstChild(&indexColumnList);

	Element indexColumn;
	indexColumnList.getFirstChild(&indexColumn);
	
	do {
		const char* columnName = indexColumn.getAttribute(COLUMN_NAME);
		inttype columnLength = indexColumn.getAttributeInt(COLUMN_LENGTH);
		inttype columnLengthBytes = indexColumn.getAttributeInt(COLUMN_LENGTH_BYTES);
		inttype columnNullBit = indexColumn.getAttributeInt(COLUMN_NULL_BIT);
		inttype columnNullOffset = indexColumn.getAttributeInt(COLUMN_NULL_OFFSET);
		boolean columnReserved = indexColumn.getAttributeBool(COLUMN_RESERVED);

		fprintf(stderr, "\t\t\tCOLUMN name=%s length=%d lengthBytes=%d nullBit=%d nullOffset=%d reserved=%d\n", columnName, columnLength, columnLengthBytes, columnNullBit, columnNullOffset, columnReserved);

	} while (indexColumn.getNextSibling(&indexColumn) != null);

//	DomUtil::writeFile(doc, "schema.xml");

	delete doc;
	
	return 0;
}

int write() {
	int success = 0;
	
	Document* doc = DomUtil::newDocument();

	Element schema;
	doc->createElement(SCHEMA, &schema);

	schema.setAttributeInt(SCHEMA_PROTOCOL, 1);
	schema.setAttribute(SCHEMA_VERSION, "v1");

	doc->appendChild(&schema);

	Element table;
	doc->createElement(TABLE, &table);

	table.setAttribute(TABLE_NAME, "t1");

	schema.appendChild(&table);

	Element indexList;
	doc->createElement(INDEX_LIST, &indexList);
	table.appendChild(&indexList);

	Element index;
	doc->createElement(INDEX, &index);

	index.setAttribute(INDEX_NAME, "idx1");
	index.setAttributeInt(INDEX_LENGTH, 10);
	index.setAttributeBool(INDEX_PRIMARY, true);
	index.setAttributeBool(INDEX_UNIQUE, true);
	index.setAttributeBool(INDEX_HIDDEN, false);

	indexList.appendChild(&index);

	Element indexColumnList;
	doc->createElement(COLUMN_LIST, &indexColumnList);
	index.appendChild(&indexColumnList);

//	while (key_parts)
	{
		Element indexColumn;
		doc->createElement(COLUMN, &indexColumn);

		indexColumn.setAttribute(COLUMN_NAME, "c1");
		indexColumn.setAttributeInt(COLUMN_LENGTH, 8);
		indexColumn.setAttributeInt(COLUMN_LENGTH_BYTES, 0);
		indexColumn.setAttributeInt(COLUMN_NULL_BIT, 0);
		indexColumn.setAttributeInt(COLUMN_NULL_OFFSET, 0);
		indexColumn.setAttributeBool(COLUMN_RESERVED, true);

		indexColumnList.appendChild(&indexColumn);
	}
	
	struct Test {
		File file;
		Test() : file("schema.xml") {}
		~Test() { file.clobber(); }
	} test;
	RandomAccessFile schemaFile(test.file, "rw");
	DomUtil::writeFile(doc, &schemaFile);
	if(schemaFile.getFilePointer() == 0) {
		DEEP_LOG(ERROR, OTHER, " -- XML: schema XML empty");
		success = 1;
	}
	
	delete doc;
	
	return success;
}

int main() {
	int success = 0;
	
	success |= read();
	success |= write();
	
	return success;
}
