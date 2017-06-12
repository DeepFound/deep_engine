/**
 e    Copyright (C) 2010 Deep Software Foundation
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
#ifndef CXX_UTIL_TREESET_CXX_
#define CXX_UTIL_TREESET_CXX_

#include "cxx/util/TreeSet.h"
#include "cxx/lang/UnsupportedOperationException.h"

using namespace cxx::util;

template<typename E, typename Cmp>
const bytetype TreeSet<E,Cmp>::INITIAL_ORDER = 3;

template<typename E, typename Cmp>
const Cmp TreeSet<E,Cmp>::COMPARATOR;

template<typename E, typename Cmp>
TreeSet<E,Cmp>::TreeSet(bytetype order, boolean delelem):
	m_comparator(&TreeSet<E,Cmp>::COMPARATOR),
	m_deleteValue(delelem) {

	initialize(order);
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::TreeSet(const Cmp* comparator, bytetype order, boolean delelem):
	m_comparator(comparator),
	m_deleteValue(delelem) {

	initialize(order);
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::~TreeSet() {
	clear();
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::initialize(bytetype order) {
	if (order < INITIAL_ORDER) {
		order = INITIAL_ORDER;
	}

	m_branchMaxIndex = order;
	m_branchLowWater = (order - 1) / 2;
	m_leafMaxIndex = (2 * (order + 1)) - 1;
	m_leafLowWater = ((m_leafMaxIndex + 1) - 1) / 2 - 1;

	m_pEntries = 0;
	m_root = null;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::notifyRootFull(void) {
	Node* oldroot = m_root;
	m_root = new Branch(this, null, oldroot);
	oldroot->split(this);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::notifyRootEmpty(void) {
	if (m_root->isLeaf() == true) {
		Leaf* root = (Leaf*) m_root;
		m_root = null;
		delete root;

	} else {
		Branch* root = (Branch*) m_root;
		m_root = root->getNode(0);
		m_root->m_parent = null;
		delete root;
	}
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::first(boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		retelem = m_root->firstLeaf()->getObject(0);
		found = true;
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::last(boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		Leaf* leaf = m_root->lastLeaf();
		retelem = leaf->getObject(leaf->m_lastIndex);
		found = true;
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::lower(const E e, boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E next = Set<E>::NULL_VALUE;
		boolean nextFound = false;

		retelem = m_root->lower(this, e, &node, &index, &next, &found, &nextFound);
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::higher(const E e, boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E prev = Set<E>::NULL_VALUE;
		boolean prevFound = false;

		retelem = m_root->higher(this, e, &node, &index, &prev, &found, &prevFound);
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::floor(const E e, boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E next = Set<E>::NULL_VALUE;
		boolean nextFound = false;

		retelem = m_root->lower(this, e, &node, &index, &next, &found, &nextFound);

		if (nextFound == true) {
			if (m_comparator->compare(next, e) == 0) {
				retelem = next;
				found = true;
			}
		}
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::ceiling(const E e, boolean* status) const {
	E retelem = Set<E>::NULL_VALUE;
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E prev = Set<E>::NULL_VALUE;
		boolean prevFound = false;

		retelem = m_root->higher(this, e, &node, &index, &prev, &found, &prevFound);

		if (prevFound == true) {
			if (m_comparator->compare(prev, e) == 0) {
				retelem = prev;
				found = true;
			}
		}
	}

	if (status != null) {
		*status = found;
	}

	return retelem;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::nextElement(Node* node, int index, Node** block, int* location) {
	E elem = node->nextElement(index, block, location);
	if (*block == null) {
		boolean status = false;
		while (node->m_parent != null) {
			int nextIndex = node->m_parent->instanceIndex(node) + 1;
			elem = node->m_parent->getElement(nextIndex, &status);
			if (status == true) {
				*block = node->m_parent;
				*location = nextIndex;
				break;
			}
			node = node->m_parent;
		}
	}

	return elem;
}

template<typename E, typename Cmp>
const boolean TreeSet<E,Cmp>::hasNextElement(Node* node, int index) {
	boolean hasNext = false;

	if (node != null) {
		int location = -1;
		Node* block = null;
		/* E elem = */ node->nextElement(index, &block, &location);
		if (block == null) {
			boolean status = false;
			while (node->m_parent != null) {
				int nextIndex = node->m_parent->instanceIndex(node) + 1;
				/* elem = */ node->m_parent->getElement(nextIndex, &status);
				if (status == true) {
					hasNext = true;
					break;
				}
				node = node->m_parent;
			}

		} else {
			hasNext = true;
		}
	}

	return hasNext;
}

template<typename E, typename Cmp>
boolean TreeSet<E,Cmp>::add(E elem, E* retelem) {
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E oldelem = m_root->find(this, elem, &node, &index, &found);

		if ((found == true) && (oldelem != elem)) {
			node->remove(this, index);

			if (retelem != null) {
				*retelem = oldelem;

			} else if ((m_deleteValue == true) && (elem != oldelem)) {
				Converter<E>::destroy(oldelem);
			}

			if (m_root == null) {
				m_root = new Leaf(this, null, elem);
				incrementEntries();

				return found;

			} else {

				boolean tempFound = false;
				m_root->find(this, elem, &node, &index, &tempFound);
			}
		}

		node->insert(this, elem, index);

	} else {
		m_root = new Leaf(this, null, elem);
		incrementEntries();
	}

	return (found == false);
}

template<typename E, typename Cmp>
boolean TreeSet<E,Cmp>::addAll(const Collection<E>* c, Collection<E>* fillc) {
	if (c == null) {
		return false;
	}

	boolean ret = false;
	Iterator<E>* iter = const_cast<Collection<E>*>(c)->iterator();
	while (iter->hasNext() == true) {
		E v = iter->next();
		if (contains(v) == false) {
			ret |= add(v);
		}
	}
	Converter<Iterator<E>*>::destroy(iter);

	return ret;
}

template<typename E, typename Cmp>
boolean TreeSet<E,Cmp>::remove(const E e, E* retelem) {
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E elem = m_root->find(this, e, &node, &index, &found);
		if (found == true) {
			node->remove(this, index);

			if (size() == 0) {
				clear();
			}

			if (retelem != null) {
				*retelem = elem;

			} else if (m_deleteValue == true) {
				Converter<E>::destroy(elem);
			}
		}
	}

	return found;
}

template<typename E, typename Cmp>
boolean TreeSet<E,Cmp>::contains(const E e, E* retelem) const {
	boolean found = false;

	if (m_root != null) {
		Node* node = null;
		int index;
		E elem = m_root->find(this, e, &node, &index, &found);
		if (retelem != null) {
			*retelem = elem;
		}
	}

	return found;
}

template<typename E, typename Cmp>
SortedSet<E>* TreeSet<E,Cmp>::headSet(const E toElement, SortedSet<E>* fillset) {
	int endIndex = -1;
	Node* endNode = null;
	if (m_root != null) {
		boolean status;
		m_root->find(this, toElement, &endNode, &endIndex, &status);
	}

	if (fillset != null) {
		// TODO: check type
		((KeySet*) fillset)->reset(this, endNode, endIndex);

	} else {
		fillset = new KeySet(this, endNode, endIndex);
	}

	return fillset;
}

template<typename E, typename Cmp>
SortedSet<E>* TreeSet<E,Cmp>::subSet(const E fromElement, const E toElement, SortedSet<E>* fillset) {
	int startIndex = -1;
	int endIndex = -1;
	Node* startNode = null;
	Node* endNode = null;
	if (m_root != null) {
		E nextElement(Set<E>::NULL_VALUE);
		boolean status, nextStatus;
		m_root->lower(this, fromElement, &startNode, &startIndex, &nextElement, &status, &nextStatus);
		m_root->find(this, toElement, &endNode, &endIndex, &status);
	}

	if (fillset != null) {
		// TODO: check type
		((KeySet*) fillset)->reset(this, startNode, startIndex, endNode, endIndex);

	} else {
		fillset = new KeySet(this, startNode, startIndex, endNode, endIndex);
	}

	return fillset;
}

template<typename E, typename Cmp>
SortedSet<E>* TreeSet<E,Cmp>::tailSet(const E fromElement, SortedSet<E>* fillset) {
	int startIndex = -1;
	Node* startNode = null;
	if (m_root != null) {
		E nextElement(Set<E>::NULL_VALUE);
		boolean status, nextStatus;
		m_root->lower(this, fromElement, &startNode, &startIndex, &nextElement, &status, &nextStatus);
	}

	if (fillset != null) {
		// TODO: check type
		((KeySet*) fillset)->reset(this, startNode, startIndex, null, -1);

	} else {
		fillset = new KeySet(this, startNode, startIndex, null, -1);
	}

	return fillset;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::clear(boolean delelem) {

	if (m_root != null) {
		if (delelem == true) {
			KeySetIterator iter(this);
			while (iter.KeySetIterator::hasNext()) {
				Converter<E>::destroy(iter.KeySetIterator::next());
			}
		}

		m_pEntries = 0;
		delete m_root;
	}

	m_root = null;
}

template<typename E, typename Cmp>
Iterator<E>* TreeSet<E,Cmp>::iterator(void) {
	return new KeySetIterator(this);
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Node::Node(Branch* parent, boolean isleaf):
	m_parent(parent),
	m_lastIndex(-1),
	#ifdef CXX_UTIL_TREE_SLOTTED
	m_slotIndex(-1),
	#endif
	m_isLeaf(isleaf) {
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Branch::Branch(TreeSet<E,Cmp>* self, Branch* parent) :
	Node(parent, false) {

	#ifdef DEEP_DEBUG
	m_items = (Item*) calloc(1, sizeof(Item) * (getMaxIndex(self) + 1));
	#else
	m_items = (Item*) malloc(sizeof(Item) * (getMaxIndex(self) + 1));
	#endif
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Branch::Branch(TreeSet<E,Cmp>* self, Branch* parent, Node* oldroot) :
	Node(parent, false) {

	#ifdef DEEP_DEBUG
	m_items = (Item*) calloc(1, sizeof(Item) * (getMaxIndex(self) + 1));
	#else
	m_items = (Item*) malloc(sizeof(Item) * (getMaxIndex(self) + 1));
	#endif

	append(oldroot, Set<E>::NULL_VALUE);
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Branch::~Branch(void) {
	if (Node::m_lastIndex > 0) {
		delete m_items[0].m_node;
	}

	for (int i = 1; i <= Node::m_lastIndex; i++) {
		delete m_items[i].m_node;
	}

	free(m_items);
	#ifdef DEEP_DEBUG
	m_items = null;
	#endif
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::insert(TreeSet<E,Cmp>* self, const E obj, int index) {
	Leaf* leaf = getNode(index - 1)->lastLeaf();
	leaf->insert(self, obj, leaf->m_lastIndex + 1);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::insert(TreeSet<E,Cmp>* self, Node* node, E obj, int index) {
	Item newitem(node, obj);

	insert(self, newitem, index);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::insertElement(Item& item, int index) {
	for (int i = Node::m_lastIndex + 1; i > index; i--) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		getItem(i).assign(i, getItem(i - 1));
		#else
		getItem(i) = getItem(i - 1);
		#endif
	}

	setItem(index, item);

	Node::m_lastIndex++;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::insertElement(Node* node, E obj, int index) {
	Item newitem(node, obj);

	insertElement(newitem, index);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::insert(TreeSet<E,Cmp>* self, Item& item, int index) {
	insertElement(item, index);

	if (isFull(self) == true) {
		notifyParent(self);
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::appendFrom(TreeSet<E,Cmp>* self, Branch* source, int begin, int end) {
	if (begin > end) {
		return;
	}

	for (int i = begin; i <= end; i++) {
		setItem(++Node::m_lastIndex, source->getItem(i));
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::append(Node* node, E obj) {
	setItem(++Node::m_lastIndex, obj, node);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::append(Item& item) {
	setItem(++Node::m_lastIndex, item);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::balanceWithLeft(TreeSet<E,Cmp>* self, Branch* lNode, int pIndex) {
	int newSize = (getVirtualEntries() + lNode->getPhysicalEntries()) / 2;
	int indexFromHere = getPhysicalEntries() - newSize;
	pushLeft(self, indexFromHere, lNode, pIndex);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::balanceWithRight(TreeSet<E,Cmp>* self, Branch* rNode, int pIndex) {
	int newSize = (getPhysicalEntries() + rNode->getVirtualEntries()) / 2;
	int indexFromHere = getPhysicalEntries() - newSize;
	pushRight(self, indexFromHere, rNode, pIndex);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::balanceWith(TreeSet<E,Cmp>* self, Branch* rNode, int pindx) {
	if (getPhysicalEntries() < rNode->getVirtualEntries()) {
		rNode->balanceWithLeft(self, this, pindx);

	} else {
		balanceWithRight(self, rNode, pindx);
	}
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Branch::find(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, boolean* status) {

	#ifdef CXX_UTIL_TREE_BSEARCH
	register int last = -1;
	register int start = 1;
	register int finish = Node::m_lastIndex;
	while (start <= finish) {
		int mid = (start + finish) >> 1;
		int weight = self->m_comparator->compare(getObject(mid), what);
		if (weight == 0) {
			*block = this;
			*location = mid;
			*status = true;
			return getObject(mid);
		}

		if (weight < 0) {
			start = mid + 1;

		} else {
			last = finish;
			finish = mid - 1;
		}
	}

	if (last != -1) {
		for (int i = start; i <= last; i++) {
			int weight = self->m_comparator->compare(getObject(i), what);
			if (weight > 0) {
				return getNode(i - 1)->find(self, what, block, location, status);
			}
		}
	}
	#else
	for (int i = 1 ; i <= Node::m_lastIndex; i++) {
		int weight = self->m_comparator->compare(getObject(i), what);
		if (weight == 0) {
			*block = this;
			*location = i;
			*status = true;
			return getObject(i);
		}

		if (weight > 0) {
			return getNode(i - 1)->find(self, what, block, location, status);
		}
	}
	#endif

	return getNode(Node::m_lastIndex)->find(self, what, block, location, status);
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Branch::lower(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, E* next, boolean* status, boolean* nextStatus) {
	for (int i = Node::m_lastIndex; i > 0; i--) {
		int weight = self->m_comparator->compare(getObject(i), what);
		if (weight < 0) {
			const E object = getNode(i)->lower(self, what, block, location, next, status, nextStatus);
			if (*status == false) {
				*block = this;
				*location = i;
				*status = true;
				return getObject(i);

			} else {
				if ((*nextStatus == false) && (i != Node::m_lastIndex)) {
					*next = getObject(i + 1);
					*nextStatus = true;
				}

				return object;
			}
		}
	}

	const E object = getNode(0)->lower(self, what, block, location, next, status, nextStatus);
	if ((*nextStatus == false) && (Node::m_lastIndex != 0)) {
		*next = getObject(1);
		*nextStatus = true;
	}

	return object;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Branch::higher(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, E* prev, boolean* status, boolean* prevStatus) {
	for (int i = 1 ; i <= Node::m_lastIndex; i++) {
		int weight = self->m_comparator->compare(getObject(i), what);
		if (weight > 0) {
			const E object = getNode(i - 1)->higher(self, what, block, location, prev, status, prevStatus);
			if (*status == false) {
				*block = this;
				*location = i;
				*status = true;
				return getObject(i);

			} else {
				if ((*prevStatus == false) && (i > 1)) {
					*prev = getObject(i - 1);
					*prevStatus = true;
				}

				return object;
			}
		}
	}

	const E object = getNode(Node::m_lastIndex)->higher(self, what, block, location, prev, status, prevStatus);
	if ((*prevStatus == false) && (Node::m_lastIndex != 0)) {
		*prev = getObject(Node::m_lastIndex);
		*prevStatus = true;
	}

	return object;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Branch::getElement(int index, boolean* status) {
	if ((index > 0) && (index <= Node::m_lastIndex)) {
		*status = true;
		return getObject(index);
	}

	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Branch::nextElement(int index, Node** block, int* location) {
	if (index <= Node::m_lastIndex) {
		*block = getNode(index)->firstLeaf();
		*location = 0;
		return ((Leaf*)*block)->getObject(0);
	}

	*block = null;
	*location = -1;
	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
int TreeSet<E,Cmp>::Branch::instanceIndex(const Node* node) const {
	#ifdef CXX_UTIL_TREE_SLOTTED
	return node->m_slotIndex;
	#else
	for (inttype i = 0; i <= Node::m_lastIndex; i++) {
		if (getNode(i) == node) {
			return i;
		}
	}

	return 0;
	#endif
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::notifyParent(TreeSet<E,Cmp>* self) {
	if (Node::m_parent != null) {
		Node::m_parent->isFull(self, this);

	} else {
		self->notifyRootFull();
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::isFull(TreeSet<E,Cmp>* self, Node* node) {
	if (node->isLeaf() == true) {
		Leaf* left = null;
		Leaf* right = null;
		Leaf* leaf = (Leaf*) node;
		int leafIndex = instanceIndex(leaf);
		boolean hasRight = (leafIndex < Node::m_lastIndex) && ((right = (Leaf*) getNode(leafIndex + 1)) != null);
		boolean hasLeft = (leafIndex > 0) && ((left = (Leaf*) getNode(leafIndex - 1)) != null);
		boolean rightFull = ((hasRight == true) && (right->isAlmostFull(self) == true));
		boolean leftFull = ((hasLeft == true) && (left->isAlmostFull(self) == true));

		if (rightFull == true) {
			if (leftFull == true) {
				left->splitWith(self, leaf, leafIndex);

			} else if (hasLeft == true) {
				leaf->balanceWithLeft(self, left, leafIndex);

			} else {
				leaf->splitWith(self, right, leafIndex + 1);
			}

		} else if (hasRight == true) {
			leaf->balanceWithRight(self, right, leafIndex + 1);

		} else if (leftFull == true) {
			left->splitWith(self, leaf, leafIndex);

		} else if (hasLeft == true) {
			leaf->balanceWithLeft(self, left, leafIndex);
		}

	} else {
		Branch* left = null;
		Branch* right = null;
		Branch* branch = (Branch*) node;
		int branchIndex = instanceIndex(branch);
		boolean hasRight = (branchIndex < Node::m_lastIndex) && ((right = (Branch*) getNode(branchIndex + 1)) != null);
		boolean hasLeft = (branchIndex > 0) && ((left = (Branch*) getNode(branchIndex - 1)) != null);
		boolean rightFull = ((hasRight == true) && (right->isAlmostFull(self) == true));
		boolean leftFull = ((hasLeft == true) && (left->isAlmostFull(self) == true));

		if (rightFull == true) {
			if (leftFull == true) {
				left->splitWith(self, branch, branchIndex);

			} else if (hasLeft == true) {
				branch->balanceWithLeft(self, left, branchIndex);

			} else {
				branch->splitWith(self, right, branchIndex + 1);
			}

		} else if (hasRight == true) {
			branch->balanceWithRight(self, right, branchIndex + 1);

		} else if (leftFull == true) {
			left->splitWith(self, branch, branchIndex);

		} else if (hasLeft == true) {
			branch->balanceWithLeft(self, left, branchIndex);
		}
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::isLow(TreeSet<E,Cmp>* self, Node* node) {
	if (node->isLeaf() == true) {
		Leaf* left = null;
		Leaf* right = null;
		Leaf* leaf = (Leaf*) node;
		int leafIndex = instanceIndex(leaf);
		boolean hasRight = (leafIndex < Node::m_lastIndex) && ((right = (Leaf*)getNode(leafIndex + 1)) != 0);
		boolean hasLeft = (leafIndex > 0) && ((left = (Leaf*) getNode(leafIndex - 1)) != 0);

		if ((hasRight == true) && (leaf->getPhysicalEntries() + right->getVirtualEntries()) >= leaf->getMaxPhysicalEntries(self)) {
			leaf->balanceWith(self, right, leafIndex + 1);

		} else if ((hasLeft == true) && (leaf->getVirtualEntries() + left->getPhysicalEntries()) >= leaf->getMaxPhysicalEntries(self)) {
			left->balanceWith(self, leaf, leafIndex);

		} else if (hasLeft == true) {
			left->mergeWithRight(self, leaf, leafIndex);

		} else if (hasRight == true) {
			leaf->mergeWithRight(self, right, leafIndex + 1);
		}

	} else {
		Branch* left = null;
		Branch* right = null;
		Branch* branch = (Branch*) node;
		int branchIndex = instanceIndex(branch);
		boolean hasRight = (branchIndex < Node::m_lastIndex) && ((right = (Branch*) getNode(branchIndex + 1)) != 0);
		boolean hasLeft = (branchIndex > 0) && ((left = (Branch*) getNode(branchIndex - 1)) != 0);

		if ((hasRight == true) && (branch->getPhysicalEntries() + right->getVirtualEntries()) >= branch->getMaxPhysicalEntries(self)) {
			branch->balanceWith(self, right, branchIndex + 1);

		} else if ((hasLeft == true) && (branch->getVirtualEntries() + left->getPhysicalEntries()) >= branch->getMaxPhysicalEntries(self)) {
			left->balanceWith(self, branch, branchIndex);

		} else if (hasLeft == true) {
			left->mergeWithRight(self, branch, branchIndex);

		} else if (hasRight == true) {
			branch->mergeWithRight(self, right, branchIndex + 1);
		}
	}
}

template<typename E, typename Cmp>
typename TreeSet<E,Cmp>::Leaf* TreeSet<E,Cmp>::Branch::firstLeaf(void) {
	return getNode(0)->firstLeaf();
}

template<typename E, typename Cmp>
typename TreeSet<E,Cmp>::Leaf* TreeSet<E,Cmp>::Branch::lastLeaf(void) {
	return getNode(Node::m_lastIndex)->lastLeaf();
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::mergeWithRight(TreeSet<E,Cmp>* self, Branch* rNode, int pIndex) {
	if (rNode->getPhysicalEntries() > 0) {
		rNode->pushLeft(self, rNode->getPhysicalEntries(), this, pIndex);
	}

	rNode->setObject(0, Node::m_parent->getObject(pIndex));

	appendFrom(self, rNode, 0, 0);

	Node::m_parent->removeItem(self, pIndex);

	delete rNode;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::pushLeft(TreeSet<E,Cmp>* self, int indexFromHere, Branch* lNode, int pIndex) {
	setObject(0, Node::m_parent->getObject(pIndex));

	lNode->appendFrom(self, this, 0, indexFromHere - 1);

	shiftLeft(self, indexFromHere);

	Node::m_parent->setObject(pIndex, getObject(0));
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::pushRight(TreeSet<E,Cmp>* self, int indexFromHere, Branch* rNode, int pIndex) {
	int begin = Node::m_lastIndex - indexFromHere + 1;
	int target = rNode->m_lastIndex + indexFromHere;
	int source = rNode->m_lastIndex;

	rNode->m_lastIndex = target;
	rNode->setObject(0, Node::m_parent->getObject(pIndex));

	while (source >= 0) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		rNode->getItem(target).assign(target, rNode->getItem(source));
		target--;
		source--;
		#else
		rNode->getItem(target--) = rNode->getItem(source--);
		#endif
	}

	for (int i = Node::m_lastIndex; i >= begin; i-- ) {
		rNode->setItem(target--, getItem(i));
	}

	Node::m_parent->setObject(pIndex, rNode->getObject(0));

	Node::m_lastIndex -= indexFromHere;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::remove(TreeSet<E,Cmp>* self, int index) {
	Leaf* leaf = getNode(index)->firstLeaf();

	setObject(index, leaf->m_objects[0]);

	leaf->removeItem(self, 0);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::removeItem(TreeSet<E,Cmp>* self, int index) {
	for (int to = index; to < Node::m_lastIndex; to++) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		m_items[to].migrate(to, m_items[to + 1]);
		#else
		m_items[to] = m_items[to + 1];
		#endif

		#ifdef DEEP_DEBUG
		m_items[to + 1].m_node = null;
		m_items[to + 1].m_object = Set<E>::NULL_VALUE;
		#endif
	}

	#ifdef DEEP_DEBUG
	m_items[Node::m_lastIndex].m_node = null;
	m_items[Node::m_lastIndex].m_object = Set<E>::NULL_VALUE;
	#endif

	Node::m_lastIndex--;

	if (isLow(self) == true) {
		if (Node::m_parent != null) {
			Node::m_parent->isLow(self, this);

		} else {
			if (getPhysicalEntries() == 0) {
				self->notifyRootEmpty();
			}
		}
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::shiftLeft(TreeSet<E,Cmp>* self, int count) {
	if (count <= 0) {
		return;
	}

	for (int i = count; i <= Node::m_lastIndex; i++) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		getItem(i - count).assign(i - count, getItem(i));
		#else
		getItem(i - count) = getItem(i);
		#endif
	}

	Node::m_lastIndex -= count;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::split(TreeSet<E,Cmp>* self) {
	Branch* nNode = new Branch(self, Node::m_parent);

	Node::m_parent->append(nNode, getObject(Node::m_lastIndex));

	nNode->appendFrom(self, this, Node::m_lastIndex, Node::m_lastIndex);

	Node::m_lastIndex--;

	balanceWithRight(self, nNode, 1);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Branch::splitWith(TreeSet<E,Cmp>* self, Branch* rNode, int kIndex) {
	rNode->setObject(0, Node::m_parent->getObject(kIndex));

	int numberOfKeys = getPhysicalEntries() + rNode->getVirtualEntries();
	int newSizeOne3rd = numberOfKeys / 3;
	int newSizeOneHalf = (numberOfKeys - newSizeOne3rd) / 2;
	int newSizeOfNode = (numberOfKeys - newSizeOne3rd - newSizeOneHalf);
	int indexFromHere = getPhysicalEntries() - newSizeOne3rd;
	int indexFromNode = rNode->getVirtualEntries() - newSizeOfNode;

	Branch* nNode = new Branch(self, Node::m_parent);
	if (indexFromHere > 0) {
		nNode->append(getItem(Node::m_lastIndex));
		Node::m_parent->insertElement(nNode, getObject(Node::m_lastIndex--), kIndex);

		if (indexFromHere > 2) {
			pushRight(self, indexFromHere - 1, nNode, kIndex);
		}

		rNode->pushLeft(self, indexFromNode, nNode, kIndex + 1);

	} else {
		nNode->append(rNode->getItem(0));

		Node::m_parent->insertElement(rNode, rNode->getObject(1), kIndex + 1);

		rNode->shiftLeft(self, 1);
		Node::m_parent->setNode(kIndex, nNode);
		rNode->pushLeft(self, indexFromNode - 1, nNode, kIndex + 1);
	}

	if (Node::m_parent->isFull(self) == true) {
		Node::m_parent->notifyParent(self);
	}
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Leaf::Leaf(TreeSet<E,Cmp>* self, Branch* parent, const E obj, boolean hasObj):
	Node(parent, true) {

	int msize = (getMaxIndex(self) + 1) * sizeof(E);
	m_objects = (E*) malloc(msize);
	memset(m_objects, 0, msize);

	if (hasObj == true) {
		m_objects[++Node::m_lastIndex] = obj;
	}
}

template<typename E, typename Cmp>
TreeSet<E,Cmp>::Leaf::~Leaf(void) {
	free(m_objects);
	#ifdef DEEP_DEBUG
	m_objects = null;
	#endif
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::insert (TreeSet<E,Cmp>* self, const E obj, int index) {
	for (int i = Node::m_lastIndex + 1; i > index ; i--) {
		m_objects[i] = m_objects[i - 1];
		#ifdef DEEP_DEBUG
		m_objects[i - 1] = Set<E>::NULL_VALUE;
		#endif
	}

	m_objects[index] = obj;
	Node::m_lastIndex++;

	self->incrementEntries();

	if (isFull(self) == true) {
		if (Node::m_parent != null) {
			Node::m_parent->isFull(self, this);

		} else {
			self->notifyRootFull();
		}
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::appendFrom(TreeSet<E,Cmp>* self, Leaf* source, int begin, int end) {
	if (begin > end) {
		return;
	}

	for (int i = begin; i <= end; i++) {
		m_objects[++Node::m_lastIndex] = source->m_objects[i];
		#ifdef DEEP_DEBUG
		source->m_objects[i] = Set<E>::NULL_VALUE;
		#endif
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::append(E obj) {
	m_objects[++Node::m_lastIndex] = obj;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Leaf::find(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, boolean* status) {

	#ifdef CXX_UTIL_TREE_BSEARCH
	register int last = -1;
	register int start = 0;
	register int finish = Node::m_lastIndex;
	while (start <= finish) {
		int mid = (start + finish) >> 1;
		int weight = self->m_comparator->compare(m_objects[mid], what);
		if (weight == 0) {
			*block = this;
			*location = mid;
			*status = true;
			return m_objects[mid];
		}

		if (weight < 0) {
			start = mid + 1;

		} else {
			last = finish;
			finish = mid - 1;
		}
	}

	if (last != -1) {
		for (int i = start; i <= last; i++) {
			int weight = self->m_comparator->compare(m_objects[i], what);
			if (weight > 0) {
				*block = this;
				*location = i;
				*status = false;
				return Set<E>::NULL_VALUE;
			}
		}
	}
	#else
	for (int i = 0; i <= Node::m_lastIndex; i++) {
		int weight = self->m_comparator->compare(m_objects[i], what);
		if (weight == 0) {
			*block = this;
			*location = i;
			*status = true;
			return m_objects[i];
		}

		if (weight > 0) {
			*block = this;
			*location = i;
			*status = false;
			return Set<E>::NULL_VALUE;
		}
	}
	#endif

	*block = this;
	*location = Node::m_lastIndex + 1;
	if (status != null) {
		*status = false;
	}
	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Leaf::lower(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, E* next, boolean* status, boolean* nextStatus) {
	*next = Set<E>::NULL_VALUE;
	*nextStatus = false;

	for (int i = Node::m_lastIndex; i >= 0; i--) {
		int weight = self->m_comparator->compare(m_objects[i], what);
		if (weight < 0) {
			*block = this;
			*location = i;
			*status = true;
			return m_objects[i];
		}

		*next = m_objects[i];
		*nextStatus = true;
	}

	*block = this;
	*location = Node::m_lastIndex + 1;
	*status = false;
	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Leaf::higher(const TreeSet<E,Cmp>* self, const E what, Node** block, int* location, E* prev, boolean* status, boolean* prevStatus) {
	*prev = Set<E>::NULL_VALUE;
	*prevStatus = false;

	for (int i = 0; i <= Node::m_lastIndex; i++) {
		int weight = self->m_comparator->compare(m_objects[i], what);
		if (weight > 0) {
			*block = this;
			*location = i;
			*status = true;
			return m_objects[i];
		}

		*prev = m_objects[i];
		*prevStatus = true;
	}

	*block = this;
	*location = Node::m_lastIndex + 1;
	*status = false;
	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Leaf::getElement(int index, boolean* status) {
	if ((index >= 0) && (index <= Node::m_lastIndex)) {
		*status = true;
		return getObject(index);
	}

	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
const E TreeSet<E,Cmp>::Leaf::nextElement(int index, Node** block, int* location) {
	if (index < Node::m_lastIndex) {
		*block = this;
		*location = ++index;
		return m_objects[index];

	}

	*block = null;
	*location = -1;
	return Set<E>::NULL_VALUE;
}

template<typename E, typename Cmp>
int TreeSet<E,Cmp>::Leaf::instanceIndex(const E node) const {
	for (int i = 0; i <= Node::m_lastIndex; i++) {
		if (m_objects[i] == node) {
			return i;
		}
	}

	throw new UnsupportedOperationException("reference not found");
}

template<typename E, typename Cmp>
typename TreeSet<E,Cmp>::Leaf* TreeSet<E,Cmp>::Leaf::firstLeaf(void) {
	return this;
}

template<typename E, typename Cmp>
typename TreeSet<E,Cmp>::Leaf* TreeSet<E,Cmp>::Leaf::lastLeaf (void) {
	return this;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::balanceWithLeft(TreeSet<E,Cmp>* self, Leaf* lNode, int pIndex) {
	int newSize = (getVirtualEntries() + lNode->getPhysicalEntries()) / 2;
	int indexFromHere = getPhysicalEntries() - newSize;
	pushLeft(self, indexFromHere, lNode, pIndex);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::balanceWithRight(TreeSet<E,Cmp>* self, Leaf* rNode, int pIndex) {
	int newSize = (getPhysicalEntries() + rNode->getVirtualEntries()) / 2;
	int indexFromHere = getPhysicalEntries() - newSize;
	pushRight(self, indexFromHere, rNode, pIndex);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::balanceWith(TreeSet<E,Cmp>* self, Leaf* rNode, int pIndex) {
	if (getPhysicalEntries() < rNode->getVirtualEntries()) {
		rNode->balanceWithLeft(self, this, pIndex);

	} else {
		balanceWithRight(self, rNode, pIndex);
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::mergeWithRight(TreeSet<E,Cmp>* self, Leaf* rNode, int pIndex) {
	rNode->pushLeft(self, rNode->getPhysicalEntries(), this, pIndex);

	append(Node::m_parent->getObject(pIndex));

	Node::m_parent->removeItem(self, pIndex);

	delete rNode;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::pushLeft(TreeSet<E,Cmp>* self, int indexFromHere, Leaf* lNode, int pIndex) {
	lNode->append(Node::m_parent->getObject(pIndex));

	if (indexFromHere > 1) {
		lNode->appendFrom(self, this, 0, indexFromHere - 2);
	}

	Node::m_parent->setObject(pIndex, m_objects[indexFromHere - 1]);

	shiftLeft(self, indexFromHere);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::pushRight(TreeSet<E,Cmp>* self, int indexFromHere, Leaf* rNode, int pIndex) {
	int begin = Node::m_lastIndex - indexFromHere + 1;
	int target = rNode->m_lastIndex + indexFromHere;
	int source = rNode->m_lastIndex;

	rNode->m_lastIndex = target;

	while (source >= 0) {
		rNode->m_objects[target--] = rNode->m_objects[source--];
		#ifdef DEEP_DEBUG
		rNode->m_objects[source + 1] = Set<E>::NULL_VALUE;
		#endif
	}

	rNode->m_objects[target--] = Node::m_parent->getObject(pIndex);

	for (int i = Node::m_lastIndex; i > begin; i--) {
		rNode->m_objects[target--] = m_objects[i];
		#ifdef DEEP_DEBUG
		m_objects[i] = Set<E>::NULL_VALUE;
		#endif
	}

	Node::m_parent->setObject(pIndex, m_objects[begin]);

	Node::m_lastIndex -= indexFromHere;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::remove(TreeSet<E,Cmp>* self, int index) {
	for (int to = index; to < Node::m_lastIndex; to++) {
		m_objects[to] = m_objects[to + 1];
		#ifdef DEEP_DEBUG
		m_objects[to + 1] = Set<E>::NULL_VALUE;
		#endif
	}

	#ifdef DEEP_DEBUG
	m_objects[Node::m_lastIndex] = Set<E>::NULL_VALUE;
	#endif

	Node::m_lastIndex--;

	self->decrementEntries();

	if (isLow(self) == true) {
		if (Node::m_parent != null) {
			Node::m_parent->isLow(self, this);

		} else {
			if (getPhysicalEntries() == 0) {
				self->notifyRootEmpty();
			}
		}
	}
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::shiftLeft(TreeSet<E,Cmp>* self, int count) {
	if (count <= 0) {
		return;
	}

	for (int i = count; i <= Node::m_lastIndex; i++) {
		m_objects[i - count] = m_objects[i];
		#ifdef DEEP_DEBUG
		m_objects[i] = Set<E>::NULL_VALUE;
		#endif
	}

	Node::m_lastIndex -= count;
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::split(TreeSet<E,Cmp>* self) {
	Leaf* nNode = new Leaf(self, Node::m_parent, Set<E>::NULL_VALUE, false);

	Node::m_parent->append(nNode, m_objects[Node::m_lastIndex--]);

	balanceWithRight(self, nNode, 1);
}

template<typename E, typename Cmp>
void TreeSet<E,Cmp>::Leaf::splitWith(TreeSet<E,Cmp>* self, Leaf* rNode, int kIndex) {
	int numberOfKeys = getPhysicalEntries() + rNode->getVirtualEntries();
	int newSizeOne3rd = numberOfKeys / 3;
	int newSizeOneHalf = (numberOfKeys - newSizeOne3rd) / 2;
	int newSizeOfNode = (numberOfKeys - newSizeOne3rd - newSizeOneHalf);
	int indexFromHere = getPhysicalEntries() - newSizeOne3rd;
	int indexFromNode = rNode->getVirtualEntries() - newSizeOfNode;

	Leaf* nNode = new Leaf(self, Node::m_parent, Set<E>::NULL_VALUE, false);

	Node::m_parent->insertElement(nNode, m_objects[Node::m_lastIndex--], kIndex);

	pushRight(self, indexFromHere - 1, nNode, kIndex);
	rNode->pushLeft(self, indexFromNode, nNode, kIndex + 1);

	if (Node::m_parent->isFull(self) == true) {
		Node::m_parent->notifyParent(self);
	}
}

#endif /*CXX_UTIL_TREESET_CXX_*/
