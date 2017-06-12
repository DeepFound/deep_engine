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
#ifndef ORG_W3C_DOM_DOCUMENT_H_
#define ORG_W3C_DOM_DOCUMENT_H_

#include "org/w3c/dom/Text.h"
#include "org/w3c/dom/Element.h"

namespace org { namespace w3c { namespace dom {

class Document : public Node {
	friend class DomUtil;

	public:
		Document() {
			m_xmlNode = new XMLDocument();
		}

		Document(XMLDocument* xmlDocument) :
			Node(xmlDocument) {
		}

		~Document() {
			// frees all resources, all memory is managed by XMLDocument
			delete ((XMLDocument*) m_xmlNode);
		}

		const char* getNodeName() {
			return "#document";
		}

		Element* createElement(const char* name, Element* retElement = null) {
			XMLElement* xmlElement = ((XMLDocument*) m_xmlNode)->NewElement(name);
			if (retElement == null) {
				retElement = new Element(xmlElement);

			} else {
				retElement->m_xmlNode = xmlElement;
			}

			return retElement;
		}

		Text* createTextNode(const char* data, Text* retText = null) {
			XMLText* xmlText = ((XMLDocument*) m_xmlNode)->NewText(data);
			if (retText == null) {
				retText = new Text(xmlText);

			} else {
				retText->m_xmlNode = xmlText;
			}

			return retText;
		}
};

} } } // namespace

#endif /*ORG_W3C_DOM_DOCUMENT_H_*/
