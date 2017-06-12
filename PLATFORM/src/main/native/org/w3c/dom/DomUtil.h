/**
 *    Copyright (C) 2010 Deep Software Foundation
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#ifndef ORG_W3C_DOM_DOMUTIL_H_
#define ORG_W3C_DOM_DOMUTIL_H_

#include "org/w3c/dom/Document.h"
#include "cxx/io/IOException.h"
#include "cxx/io/RandomAccessFile.h"
#include "cxx/util/Logger.h"

using namespace cxx::util;

namespace org { namespace w3c { namespace dom {

class DomUtil : public Object {

	private:
		DomUtil() {
		}

	public:
		static Document* newDocument() {
			return new Document();
		}

		static Document* readFile(const char* filename) {
			Document* document = new Document();

			((XMLDocument*) document->m_xmlNode)->LoadFile(filename);

			return document;
		}

		static Document* readFile(FILE* file) {
			Document* document = new Document();

			((XMLDocument*) document->m_xmlNode)->LoadFile(file);

			return document;
		}
		
		static Document* readFile(RandomAccessFile* file) {
			struct Finally {
				RandomAccessFile* m_file;
				Finally(RandomAccessFile* file) { m_file = file; }
				~Finally() { m_file->syncFilePointer(); }
			} finalizer(file);
			
			return readFile(file->getHandle());
		}

		static Document* parse(const char* str) {
			Document* document = new Document();

			((XMLDocument*) document->m_xmlNode)->Parse(str);

			return document;
		}

		static void writeFile(Document* document, const char* filename) {
			const XMLError err = ((XMLDocument*) document->m_xmlNode)->SaveFile(filename);
			if (err != XML_SUCCESS) {
				DEEP_LOG(ERROR, OTHER, "Error (%s) writing XML to file: %s\n", getErrorName(err), filename);
				throw IOException("Error writing XML to file");
			}
		}

		static void writeFile(Document* document, FILE* file) {
			const XMLError err = ((XMLDocument*) document->m_xmlNode)->SaveFile(file);
			if (err != XML_SUCCESS) {
				DEEP_LOG(ERROR, OTHER, "Error (%s) writing XML to file: fd=%d\n", getErrorName(err), fileno(file));
				throw IOException("Error writing XML to file");
			}
		}
		
		static void writeFile(Document* document, RandomAccessFile* file) {
			struct Finally {
				RandomAccessFile* m_file;
				Finally(RandomAccessFile* file) { m_file = file; }
				~Finally() { m_file->syncFilePointer(); }
			} finalizer(file);
			
			writeFile(document, file->getHandle());
		}

		static void print(Document* document) {
			((XMLDocument*) document->m_xmlNode)->Print();
		}
		
	private:
		static const char* getErrorName(XMLError e) {
			switch (e) {
			case XML_SUCCESS: return "XML_SUCCESS";
			case XML_NO_ATTRIBUTE: return "XML_NO_ATTRIBUTE";
			case XML_WRONG_ATTRIBUTE_TYPE: return "XML_WRONG_ATTRIBUTE_TYPE";
			case XML_ERROR_FILE_NOT_FOUND: return "XML_ERROR_FILE_NOT_FOUND";
			case XML_ERROR_FILE_COULD_NOT_BE_OPENED: return "XML_ERROR_FILE_COULD_NOT_BE_OPENED";
			case XML_ERROR_FILE_READ_ERROR: return "XML_ERROR_FILE_READ_ERROR";
			case XML_ERROR_ELEMENT_MISMATCH: return "XML_ERROR_ELEMENT_MISMATCH";
			case XML_ERROR_PARSING_ELEMENT: return "XML_ERROR_PARSING_ELEMENT";
			case XML_ERROR_PARSING_ATTRIBUTE: return "XML_ERROR_PARSING_ATTRIBUTE";
			case XML_ERROR_IDENTIFYING_TAG: return "XML_ERROR_IDENTIFYING_TAG";
			case XML_ERROR_PARSING_TEXT: return "XML_ERROR_PARSING_TEXT";
			case XML_ERROR_PARSING_CDATA: return "XML_ERROR_PARSING_CDATA";
			case XML_ERROR_PARSING_COMMENT: return "XML_ERROR_PARSING_COMMENT";
			case XML_ERROR_PARSING_DECLARATION: return "XML_ERROR_PARSING_DECLARATION";
			case XML_ERROR_PARSING_UNKNOWN: return "XML_ERROR_PARSING_UNKNOWN";
			case XML_ERROR_EMPTY_DOCUMENT: return "XML_ERROR_EMPTY_DOCUMENT";
			case XML_ERROR_MISMATCHED_ELEMENT: return "XML_ERROR_MISMATCHED_ELEMENT";
			case XML_ERROR_PARSING: return "XML_ERROR_PARSING";
			case XML_CAN_NOT_CONVERT_TEXT: return "XML_CAN_NOT_CONVERT_TEXT";
			case XML_NO_TEXT_NODE: return "XML_NO_TEXT_NODE";
			default: return "UNKNOWN";
			}
		}
};

} } } // namespace

#endif /*ORG_W3C_DOM_DOMUTIL_H_*/
