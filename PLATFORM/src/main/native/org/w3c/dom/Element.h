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
#ifndef ORG_W3C_DOM_ELEMENT_H_
#define ORG_W3C_DOM_ELEMENT_H_

#include "org/w3c/dom/Node.h"

namespace org { namespace w3c { namespace dom {

class Element : public Node {
	friend class Node;
	friend class Document;

	public:
		Element() {
		}

		Element(XMLElement* xmlElement) :
			Node(xmlElement) {
		}
		
		~Element() {
			// nothing to do
		}

		const char* getNodeName() {
			return ((XMLElement*) m_xmlNode)->Value();
		}

		boolean hasAttribute(const char* name) const {
			return (((XMLElement*) m_xmlNode)->Attribute(name) != null);
		}

		const char* getAttribute(const char* name) const {
			return ((XMLElement*) m_xmlNode)->Attribute(name);
		}

		void setAttribute(const char* name, const char* value) {
			((XMLElement*) m_xmlNode)->SetAttribute(name, value);
		}

		inttype getAttributeInt(const char* name) const {
			return ((XMLElement*) m_xmlNode)->IntAttribute(name);
		}

		void setAttributeInt(const char* name, inttype value) {
			((XMLElement*) m_xmlNode)->SetAttribute(name, value);
		}
		
		uinttype getAttributeUint(const char* name) const {
			return ((XMLElement*) m_xmlNode)->UnsignedAttribute(name);
		}

		void setAttributeUint(const char* name, uinttype value) {
			((XMLElement*) m_xmlNode)->SetAttribute(name, value);
		}

		ulongtype getAttributeUlong(const char* name) const {
			ulongtype value = 0;

			const char* str = getAttribute(name);
			sscanf(str, "%llu", &value);

			return value;
		}

		void setAttributeUlong(const char* name, ulongtype value) {
			char str[64];

			sprintf(str, "%llu", value);

			((XMLElement*) m_xmlNode)->SetAttribute(name, str);
		}

		longtype getAttributeLong(const char* name) const {
			longtype value = 0;

			const char* str = getAttribute(name);
			sscanf(str, "%lld", &value);

			return value;
		}

		void setAttributeLong(const char* name, longtype value) {
			char str[64];

			sprintf(str, "%lld", value);

			((XMLElement*) m_xmlNode)->SetAttribute(name, str);
		}

		boolean getAttributeBool(const char* name) const {
			return ((XMLElement*) m_xmlNode)->BoolAttribute(name);
		}

		void setAttributeBool(const char* name, bool value) {
			((XMLElement*) m_xmlNode)->SetAttribute(name, value);
		}

		template<typename T>
		void setAttribute(const char* name, const T value, const char* (*toString) (T value)) {
			const char* s = toString(value);
			setAttribute(name, s);
			delete[] s;
		}
		
		template<typename T>
		T getAttribute(const char* name, T (*fromString) (const char* value)) const {
			return fromString(getAttribute(name));
		}
};

} } } // namespace

#endif /*ORG_W3C_DOM_ELEMENT_H_*/
