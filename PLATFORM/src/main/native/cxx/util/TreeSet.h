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
#ifndef CXX_UTIL_TREESET_H_
#define CXX_UTIL_TREESET_H_

#include "cxx/lang/Comparable.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "cxx/util/SortedSet.h"
#include "cxx/util/Comparator.h"

namespace cxx { namespace util {

template<typename E, typename Cmp = Comparator<E> >
class TreeSet : /* public NavigableSet */ public SortedSet<E> {
	class Leaf;
	class Branch;

	class Node {

		private:
			Branch* m_parent;
			shorttype m_lastIndex;
			#ifdef CXX_UTIL_TREE_SLOTTED
			shorttype m_slotIndex;
			#endif
			boolean m_isLeaf : 1;

		public:
			Node(Branch* parent, boolean isleaf);

			virtual ~Node(void) {
			}

			virtual void insert(TreeSet<E,Cmp>* self, const E obj, int index) = 0;
			virtual void remove(TreeSet<E,Cmp>* self, int index) = 0;

			virtual const E find(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, boolean* status) = 0;
			virtual const E lower(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* next, boolean* status, boolean* nextStatus) = 0;
			virtual const E higher(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* prev, boolean* status, boolean* prevStatus) = 0;

			virtual const E getElement(int index, boolean* status) = 0;
			virtual const E nextElement(int index, Node** block, int* location) = 0;

			virtual Leaf* firstLeaf(void) = 0;
			virtual Leaf* lastLeaf(void) = 0;

			virtual void split(TreeSet<E,Cmp>* self) = 0;

			FORCE_INLINE boolean isLeaf() const {
				return m_isLeaf;
			}

		friend class TreeSet;
		friend class Branch;
		friend class Leaf;
	};

	class Item {

		private:
			Node* m_node;
			E m_object;

		public:
			Item(void):
				m_node(null),
				m_object(Set<E>::NULL_VALUE) {  // XXX: to be fixed
			}

			Item(Node* node, E object):
				m_node(node),
				m_object(object) {
			}

			#ifdef CXX_UTIL_TREE_SLOTTED
			FORCE_INLINE void assign(inttype index, Item& item) {
				m_node = item.m_node;
				m_object = item.m_object;
				m_node->m_slotIndex = index;
			}

			FORCE_INLINE void migrate(inttype index, Item& item) {
				assign(index, item);
				item.m_node = null;
				item.m_object = Set<E>::NULL_VALUE;
			}
			#endif

		friend class TreeSet;
		friend class Branch;
	};

	class Branch : public Node {

		private:
			Item* m_items;

		public:
			Branch(TreeSet<E,Cmp>* self, Branch* parent);
			Branch(TreeSet<E,Cmp>* self, Branch* parent, Node* oldroot);

			virtual ~Branch(void);

			void insert(TreeSet<E,Cmp>* self, const E obj, int index);
			void insert(TreeSet<E,Cmp>* self, Item& item, int index);
			void insert(TreeSet<E,Cmp>* self, Node* node, E obj, int index);
			void insertElement(Item& item, int index);
			void insertElement(Node* node, E obj, int index);

			void remove(TreeSet<E,Cmp>* self, int index);
			void removeItem(TreeSet<E,Cmp>* self, int index);

			const E find(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, boolean* status);
			const E lower(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* next, boolean* status, boolean* nextStatus);
			const E higher(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* prev, boolean* status, boolean* prevStatus);

			const E getElement(int index, boolean* status);
			const E nextElement(int index, Node** block, int* location);

			FORCE_INLINE void setNode(int index, Node* node) {
				#ifdef CXX_UTIL_TREE_SLOTTED
				node->m_slotIndex = index;
				#endif

				m_items[index].m_node = node;
				node->m_parent = this;
			}

			FORCE_INLINE void setObject(int index, E obj) {
				m_items[index].m_object = obj;
			}

			FORCE_INLINE void setItem(int index, Item& item) {
				#ifdef CXX_UTIL_TREE_SLOTTED
				m_items[index].assign(index, item);
				#else
				m_items[index] = item;
				#endif

				item.m_node->m_parent = this;
			}

			FORCE_INLINE void setItem(int index, E obj, Node* node) {
				setNode(index, node);
				setObject(index, obj);
			}

			void notifyParent(TreeSet<E,Cmp>* self);

			Leaf* lastLeaf(void);
			Leaf* firstLeaf(void);

			FORCE_INLINE Item& getItem(int index) const {
				return m_items[index];
			}

			FORCE_INLINE Node* getNode(int index) const {
				return m_items[index].m_node;
			}

			FORCE_INLINE E getObject(int index) const {
				return m_items[index].m_object;
			}

			int instanceIndex(const Node* node) const;

			void split(TreeSet<E,Cmp>* self);
			void shiftLeft(TreeSet<E,Cmp>* self, int count);
			void splitWith(TreeSet<E,Cmp>* self, Branch* node, int index);

			void append(Item& item);
			void append(Node* node, E obj);
			void appendFrom(TreeSet<E,Cmp>* self, Branch* source, int begin, int end);

			void mergeWithRight(TreeSet<E,Cmp>* self, Branch* rNode, int index);

			void balanceWithLeft(TreeSet<E,Cmp>* self, Branch* lNode, int index);
			void balanceWithRight(TreeSet<E,Cmp>* self, Branch* rNode, int index);
			void balanceWith(TreeSet<E,Cmp>* self, Branch* node, int index);

			void pushLeft(TreeSet<E,Cmp>* self, int count, Branch* lNode, int pIndex);
			void pushRight(TreeSet<E,Cmp>* self, int count, Branch* rNode, int pIndex);

			FORCE_INLINE int getMaxIndex(TreeSet<E,Cmp>* self) const {
				return self->m_branchMaxIndex;
			}

			FORCE_INLINE int getPhysicalEntries(void) const {
				return Node::m_lastIndex;
			}

			FORCE_INLINE int getVirtualEntries(void) const {
				return getPhysicalEntries() + 1;
			}

			FORCE_INLINE int getMaxPhysicalEntries(TreeSet<E,Cmp>* self) const {
				return self->m_branchMaxIndex;
			}

			void isFull(TreeSet<E,Cmp>* self, Node* node);

			FORCE_INLINE int isFull(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex == getMaxIndex(self);
			}

			FORCE_INLINE int isAlmostFull(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex >= (getMaxIndex(self) - 1);
			}

			void isLow(TreeSet<E,Cmp>* self, Node* node);

			FORCE_INLINE int isLow(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex < self->m_branchLowWater;
			}

		friend class TreeSet;
		friend class Leaf;
	};

	class Leaf : public Node {

		private:
			E* m_objects;

		public:
			Leaf(TreeSet* self, Branch* parent, const E obj, boolean hasObj = true);

			virtual ~Leaf(void);

			void insert(TreeSet<E,Cmp>* self, const E obj, int index);

			void remove(TreeSet<E,Cmp>* self, int index);
			FORCE_INLINE void removeItem(TreeSet<E,Cmp>* self, int index) {
				remove(self, index);
			}

			const E find(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, boolean* status);
			const E lower(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* next, boolean* status, boolean* nextStatus);
			const E higher(const TreeSet<E,Cmp>* self, const E elem, Node** node, int* index, E* prev, boolean* status, boolean* prevStatus);

			const E getElement(int index, boolean* status);
			const E nextElement(int index, Node** block, int* location);

			FORCE_INLINE E getObject(int index ) {
				return m_objects[index];
			}

			FORCE_INLINE void setObject(int index, E obj) {
				m_objects[index] = obj;
			}

			Leaf* firstLeaf(void);
			Leaf* lastLeaf(void);

			int instanceIndex(const E obj) const;

			void split(TreeSet<E,Cmp>* self);
			void shiftLeft(TreeSet<E,Cmp>* self, int count);
			void splitWith(TreeSet<E,Cmp>* self, Leaf* rNode, int index);

			void append(E obj);
			void appendFrom(TreeSet<E,Cmp>* self, Leaf* source, int begin, int end);

			void mergeWithRight(TreeSet<E,Cmp>* self, Leaf* rNode, int index);

			void balanceWith(TreeSet<E,Cmp>* self, Leaf* node, int index);
			void balanceWithLeft(TreeSet<E,Cmp>* self, Leaf* lNode, int index);
			void balanceWithRight(TreeSet<E,Cmp>* self, Leaf* rNode, int index);

			void pushLeft(TreeSet<E,Cmp>* self, int count, Leaf* lNode, int pIndex);
			void pushRight(TreeSet<E,Cmp>* self, int count, Leaf* rNode, int pIndex);

			FORCE_INLINE int getMaxIndex(TreeSet<E,Cmp>* self) const {
				return self->m_leafMaxIndex;
			}

			FORCE_INLINE int getPhysicalEntries(void) const {
				return Node::m_lastIndex + 1;
			}

			FORCE_INLINE int getVirtualEntries(void) const {
				return getPhysicalEntries() + 1;
			}

			FORCE_INLINE int getMaxPhysicalEntries(TreeSet<E,Cmp>* self) const {
				return self->m_leafMaxIndex + 1;
			}

			FORCE_INLINE int isFull(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex == getMaxIndex(self);
			}

			FORCE_INLINE int isAlmostFull(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex >= (getMaxIndex(self) -1);
			}

			FORCE_INLINE int isLow(TreeSet<E,Cmp>* self) const {
				return Node::m_lastIndex < self->m_leafLowWater;
			}

		friend class TreeSet;
		friend class Branch;
	};

	private:
		Node* m_root;
		const Cmp* m_comparator;

		inttype m_pEntries;
		ubytetype m_leafLowWater;
		ubytetype m_leafMaxIndex;
		ubytetype m_branchLowWater;
		ubytetype m_branchMaxIndex;
		boolean m_deleteValue : 1;

		static const Cmp COMPARATOR;

	private:
		void initialize(bytetype order);
		void notifyRootFull(void);
		void notifyRootEmpty(void);

		FORCE_INLINE void incrementEntries(void) {
			m_pEntries++;
		}

		FORCE_INLINE void decrementEntries(void) {
			m_pEntries--;
		}

		const E nextElement(Node* node, int index, Node** block, int* location);
		const boolean hasNextElement(Node* node, int index);

		int indexOf(const E obj) const;

	public:
		FORCE_INLINE void setDeleteValue(boolean flag) {
			m_deleteValue = flag;
		}

		FORCE_INLINE boolean getDeleteValue() const {
			return m_deleteValue;
		}

		// XXX: used for extreme optimization
		FORCE_INLINE void transfer(TreeSet* tree) {
			tree->m_root = m_root;
			tree->m_pEntries = m_pEntries;
			m_pEntries = 0;
			m_root = null;
		}

	public:
		static const bytetype INITIAL_ORDER;

		/*
		TreeSet(const Collecton* c);
		TreeSet(const SortedSet* s);
		*/
		TreeSet(bytetype order = INITIAL_ORDER /* XXX: purposely signed */, boolean deleteValue = false);
		TreeSet(const Cmp* comparator, bytetype order = INITIAL_ORDER, boolean deleteValue = false);
		virtual ~TreeSet();

		virtual const E first(boolean* status) const;
		virtual const E first(void) const {
			return first(null);
		}

		virtual const E last(boolean* status) const;
		virtual const E last(void) const {
			return last(null);
		}

		virtual const E lower(const E e, boolean* status) const;
		virtual const E lower(const E e) const {
			return lower(e, null);
		}

		virtual const E higher(const E e, boolean* status) const;
		virtual const E higher(const E e) const {
			return higher(e, null);
		}

		virtual const E floor(const E e, boolean* status) const;
		virtual const E floor(const E e) const {
			return floor(e, null);
		}

		virtual const E ceiling(const E e, boolean* status) const;
		virtual const E ceiling(const E e) const {
			return ceiling(e, null);
		}

		virtual boolean add(E elem, E* retelem);
		virtual boolean add(E elem) {
			return add(elem, null);
		}

		virtual boolean addAll(const Collection<E>* c, Collection<E>* fillc);
		virtual boolean addAll(const Collection<E>* c) {
			return addAll(c, null);
		}

		virtual boolean remove(const E key, E* retelem);
		virtual boolean remove(const E key) {
			return remove(key, null);
		}

		virtual boolean contains(const E key, E* retelem) const;
		virtual boolean contains(const E key) const {
			return contains(key, null);
		}

		virtual SortedSet<E>* headSet(const E toElement, SortedSet<E>* fillset);
		virtual SortedSet<E>* headSet(const E toElement) {
			return headSet(toElement, null);
		}

		virtual SortedSet<E>* subSet(const E fromElement, const E toElement, SortedSet<E>* fillset);
		virtual SortedSet<E>* subSet(const E fromElement, const E toElement) {
			return subSet(fromElement, toElement, null);
		}

		virtual SortedSet<E>* tailSet(const E fromElement, SortedSet<E>* fillset);
		virtual SortedSet<E>* tailSet(const E fromElement) {
			return tailSet(fromElement, null);
		}

		virtual boolean isEmpty() const {
			return (size() == 0);
		}

		virtual int size() const {
			return m_pEntries;
		}

		virtual void clear(boolean deleteValue);
		virtual void clear() {
			clear(m_deleteValue);
		}

		virtual Iterator<E>* iterator();

	class KeySet;

	class KeySetIterator: public Iterator<E> {

		private:
			TreeSet<E,Cmp>* m_set;
			Node* m_startNode;
			Node* m_endNode;
			E m_elem;
			int m_startIndex;
			int m_endIndex;

			KeySetIterator(void):
				m_set(null),
				m_startNode(null),
				m_endNode(null),
				m_elem(Set<E>::NULL_VALUE),
				m_startIndex(-1),
				m_endIndex(-1) {
			}

			KeySetIterator(TreeSet<E,Cmp>* set) :
				m_elem(Set<E>::NULL_VALUE) {
				reset(set);
			}

			KeySetIterator(TreeSet<E,Cmp>* set, Node* startNode, int startIndex, Node* endNode, int endIndex) :
				m_elem(Set<E>::NULL_VALUE) {
				reset(set, startNode, startIndex, endNode, endIndex);
			}

			void reset(TreeSet<E,Cmp>* set) {
				reset(set, null, -1);
			}

			void reset(TreeSet<E,Cmp>* set, Node* endNode, int endIndex) {
				reset(set, null, -1, endNode, endIndex);
			}

			void reset(TreeSet<E,Cmp>* set, Node* startNode, int startIndex, Node* endNode, int endIndex) {
				if (startNode == null) {
					m_startNode = set->m_root;
					if (m_startNode != null) {
						m_startIndex = m_startNode->isLeaf() ? -1 : 0;
					}

				} else {
					m_startNode = startNode;
					m_startIndex = startIndex;
				}

				m_elem = Set<E>::NULL_VALUE;
				m_set = set;
				m_endNode = endNode;
				m_endIndex = endIndex;
			}

			boolean end() {
				return ((m_endNode != null) && (m_startNode == m_endNode) && (m_startIndex == m_endIndex));
			}

		public:
			virtual ~KeySetIterator() {
			}

			virtual boolean hasNext() {
				if (end() == true) {
					return false;

				} else {
					return m_set->hasNextElement(m_startNode, m_startIndex);
				}
			}

			virtual const E next() {
				// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
				if (end() == true) {
					return Set<E>::NULL_VALUE;

				} else {
					m_elem = m_set->nextElement(m_startNode, m_startIndex, &m_startNode, &m_startIndex);
					return m_elem;
				}
			}

			virtual void remove() {
				boolean status = false;
				E lowerElement = m_set->lower(m_elem, &status);

				m_set->remove(m_elem);
				if (status == false) {
					reset(m_set, m_endNode, m_endIndex);

				} else {
					m_set->m_root->find(m_set, lowerElement, &m_startNode, &m_startIndex, &status);
				}
			}

		friend class TreeSet;
		friend class KeySet;
	};

	class KeySet : public SortedSet<E> {

		private:
			KeySetIterator m_iterator;
			Node* m_startNode;
			Node* m_endNode;
			inttype m_startIndex;
			inttype m_endIndex;
			boolean m_reuse : 1;

			KeySet();

			KeySet(TreeSet<E,Cmp>* set):
				m_reuse(false) {
				reset(set);
			}

			KeySet(TreeSet<E,Cmp>* set, Node* endNode, int endIndex):
				m_reuse(false) {
				reset(set, endNode, endIndex);
			}

			KeySet(TreeSet<E,Cmp>* set, Node* startNode, int startIndex, Node* endNode, int endIndex):
				m_reuse(false) {
				reset(set, startNode, startIndex, endNode, endIndex);
			}

			void reset(TreeSet<E,Cmp>* set) {
				reset(set, null, -1);
			}

			void reset(TreeSet<E,Cmp>* set, Node* endNode, int endIndex) {
				reset(set, null, -1, endNode, endIndex);
			}

			void reset(TreeSet<E,Cmp>* set, Node* startNode, int startIndex, Node* endNode, int endIndex) {
				if (startNode == null) {
					m_startNode = set->m_root;
					if (m_startNode != null) {
						m_startIndex = m_startNode->isLeaf() ? -1 :0;
					}

				} else {
					m_startNode = startNode;
					m_startIndex = startIndex;
				}

				m_endNode = endNode;
				m_endIndex = endIndex;
				m_iterator.reset(set, startNode, startIndex, endNode, endIndex);
			}

		public:
			KeySet(boolean reuse):
				m_reuse(reuse)  {
			}

			virtual ~KeySet(void) {
			}

			const E first(void) const {
				E elem = Set<E>::NULL_VALUE;

				if (m_startNode != null) {
					int index;
					Node* node;
					elem = m_iterator.m_set->nextElement(m_startNode, m_startIndex, &node, &index);

				} else {
					elem = m_iterator.m_set->first();
				}

				return elem;
			}

			const E last(void) const {
				E elem = Set<E>::NULL_VALUE;

				if (m_endNode != null) {
					boolean status;
					elem = m_endNode->getElement(m_endIndex, &status);

				} else {
					elem = m_iterator.m_set->last();
				}

				return elem;
			}

			virtual boolean add(E elem) {
				throw new UnsupportedOperationException("add not supported");
			}

			virtual boolean contains(const E elem) const {
				boolean status = false;

				inttype firstCmp = TreeSet<E,Cmp>::COMPARATOR.compare(elem, first());
				inttype lastCmp = TreeSet<E,Cmp>::COMPARATOR.compare(elem, last());
				if ((firstCmp >= 0) && (lastCmp <= 0)) {
					status = m_iterator.m_set->contains(elem);
				}

				return status;
			}

			virtual boolean remove(const E elem) {
				throw new UnsupportedOperationException("remove not supported");
			}

			virtual boolean isEmpty() const {
				return (size() == 0);
			}

			virtual int size() const {
				throw new UnsupportedOperationException("size not supported");
			}

			virtual void clear() {
				throw new UnsupportedOperationException("clear not supported");
			}

			virtual SortedSet<E>* headSet(const E toElement) {
				throw new UnsupportedOperationException("headSet not supported");
			}

			virtual SortedSet<E>* subSet(const E fromElement, const E toElement) {
				throw new UnsupportedOperationException("subSet not supported");
			}

			virtual SortedSet<E>* tailSet(const E fromElement) {
				throw new UnsupportedOperationException("tailSet not supported");
			}

			virtual Iterator<E>* iterator() {
				if (m_reuse == true) {
					return &m_iterator;

				} else {
					return new KeySetIterator(m_iterator.m_set, m_startNode, m_startIndex, m_endNode, m_endIndex);
				}
			}

			Iterator<E>* reset() {
				reset(m_iterator.m_set, null, -1);
				return &m_iterator;
			}

		friend class TreeSet;
	};
};

} } // namespace

#endif /*CXX_UTIL_TREESET_H_*/
