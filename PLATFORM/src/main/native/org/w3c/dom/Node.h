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
#ifndef ORG_W3C_DOM_NODE_H_
#define ORG_W3C_DOM_NODE_H_

#include "cxx/lang/Object.h"
#include "cxx/lang/String.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "tinyxml2.h"

using namespace tinyxml2;

using namespace cxx::lang;

namespace org { namespace w3c { namespace dom {

class Node : public Object {
	protected:
		XMLNode* m_xmlNode;

		static Node* createNode(XMLNode* node);

	public:
		Node() :
			m_xmlNode(null) {
		}

		Node(XMLNode* xmlNode) :
			m_xmlNode(xmlNode) {
		}
		
		virtual ~Node() {
			// nothing to do
		}

		virtual const char* getNodeName() {
			return null;
		};

		virtual const char* getNodeValue() {
			return null;
		};

		Node* getFirstChild(Node* retNode = null) {
			XMLNode* child = m_xmlNode->FirstChild();
			if (child == null) {
				return null;

			} else {
				if (retNode == null) {
					retNode = createNode(child);

				} else {
					retNode->m_xmlNode = child;
				}

				return retNode;
			}
		}

		Node* getLastChild(Node* retNode = null) {
			XMLNode* child = m_xmlNode->LastChild();
			if (child == null) {
				return null;

			} else {
				if (retNode == null) {
					retNode = createNode(child);

				} else {
					retNode->m_xmlNode = child;
				}

				return retNode;
			}
		}

		Node* getNextSibling(Node* retNode = null) {
			XMLNode* sibling = m_xmlNode->NextSibling();
			if (sibling == null) {
				return null;

			} else {
				if (retNode == null) {
					retNode = createNode(sibling);

				} else {
					retNode->m_xmlNode = sibling;
				}

				return retNode;
			}
		}

		Node* getPreviousSibling(Node* retNode = null) {
			XMLNode* sibling = m_xmlNode->PreviousSibling();
			if (sibling == null) {
				return null;

			} else {
				if (retNode == null) {
					retNode = createNode(sibling);

				} else {
					retNode->m_xmlNode = sibling;
				}

				return retNode;
			}
		}

		Node* appendChild(Node* node, Node* retNode = null) {
			XMLNode* child = m_xmlNode->InsertEndChild(node->m_xmlNode);
			if (child == null) {
				return null;

			} else {
				if (retNode == null) {
					retNode = createNode(child);

				} else {
					retNode->m_xmlNode = child;
				}

				return retNode;
			}
		}
};

} } } // namespace

#endif /*CXX_DOM_NODE_H_*/
