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
#ifndef CXX_UTIL_TREEMAP_H_
#define CXX_UTIL_TREEMAP_H_

#include "cxx/lang/Comparable.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "cxx/util/Comparator.h"
#include "cxx/util/SortedMap.h"
#include "cxx/util/SortedSet.h"
#include "cxx/util/TreeIterator.h"

namespace cxx { namespace util {

template<typename K, typename V, typename Ctx = void*>
class TreeMap : /* public NavigableMap */ public SortedMap<K,V,Ctx> {
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

			inline virtual ~Node(void) {
			}

			virtual void insert(TreeMap<K,V,Ctx>* self, const MapEntry<K,V,Ctx>* obj, inttype index, boolean sequential) = 0;
			virtual void remove(TreeMap<K,V,Ctx>* self, inttype index) = 0;

			#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
			virtual const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, inttype* pos = null, boolean* end = null) = 0;
			#else
			virtual const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*) = 0;
			#endif
			virtual const MapEntry<K,V,Ctx>* lower(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**) = 0;
			virtual const MapEntry<K,V,Ctx>* higher(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**) = 0;

			virtual const MapEntry<K,V,Ctx>* getEntry(inttype index) = 0;
			virtual const MapEntry<K,V,Ctx>* nextEntry(inttype index, Node** block, inttype* location) = 0;
			virtual const MapEntry<K,V,Ctx>* previousEntry(inttype index, Node** block, inttype* location) = 0;

			virtual Leaf* firstLeaf(void) = 0;
			virtual Leaf* lastLeaf(void) = 0;

			virtual void split(TreeMap<K,V,Ctx>* self) = 0;

			FORCE_INLINE boolean isLeaf() const {
				return m_isLeaf;
			}

		friend class TreeMap;
		friend class Branch;
		friend class Leaf;
	};

	class Item {

		private:
			Node* m_node;
			MapEntry<K,V,Ctx>* m_object;

		public:
			Item(void):
				m_node(null),
				m_object(null) {
			}

			Item(Node* node, MapEntry<K,V,Ctx>* object):
				m_node(node),
				m_object(object) {
			}

			#ifdef CXX_UTIL_TREE_SLOTTED
			FORCE_INLINE void assign(inttype index, Item& item) {
				m_node = item.m_node;
				m_object = item.m_object;
				m_node->m_slotIndex = index;
			}
			#endif

		friend class Branch;
	};

	class Branch : public Node {

		private:
			Item* m_items;

		public:
			Branch(TreeMap<K,V,Ctx>* self, Branch* parent);
			Branch(TreeMap<K,V,Ctx>* self, Branch* parent, Node* oldroot);

			virtual ~Branch(void);

			void insert(TreeMap<K,V,Ctx>* self, const MapEntry<K,V,Ctx>* obj, inttype index, boolean sequential);
			void insert(TreeMap<K,V,Ctx>* self, Item& item, inttype index);
			void insert(TreeMap<K,V,Ctx>* self, Node* node, MapEntry<K,V,Ctx>* obj, inttype index);
			void insertElement(Item& item, inttype index);
			void insertElement(Node* node, MapEntry<K,V,Ctx>* obj, inttype index);

			void remove(TreeMap<K,V,Ctx>* self, inttype index);
			void removeItem(TreeMap<K,V,Ctx>* self, inttype index);

			#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
			const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, inttype* pos = null, boolean* end = null);
			#else
			const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*);
			#endif
			const MapEntry<K,V,Ctx>* lower(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**);
			const MapEntry<K,V,Ctx>* higher(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**);

			const MapEntry<K,V,Ctx>* getEntry(inttype index);
			const MapEntry<K,V,Ctx>* nextEntry(inttype index, Node** block, inttype* location);
			const MapEntry<K,V,Ctx>* previousEntry(inttype index, Node** block, inttype* location);

			FORCE_INLINE void setNode(inttype index, Node* node) {
				#ifdef CXX_UTIL_TREE_SLOTTED
				node->m_slotIndex = index;
				#endif

				m_items[index].m_node = node;
				node->m_parent = this;
			}

			FORCE_INLINE void setObject(inttype index, MapEntry<K,V,Ctx>* obj) {
				m_items[index].m_object = obj;
			}

			FORCE_INLINE void setItem(inttype index, Item& item) {
				#ifdef CXX_UTIL_TREE_SLOTTED
				m_items[index].assign(index, item);
				#else
				m_items[index] = item;
				#endif

				item.m_node->m_parent = this;
			}

			FORCE_INLINE void setItem(inttype index, MapEntry<K,V,Ctx>* obj, Node* node) {
				setNode(index, node);
				setObject(index, obj);
			}

			void notifyParent(TreeMap<K,V,Ctx>* self);

			Leaf* lastLeaf(void);
			Leaf* firstLeaf(void);

			FORCE_INLINE Item& getItem(inttype index) const {
				return m_items[index];
			}

			FORCE_INLINE Node* getNode(inttype index) const {
				return m_items[index].m_node;
			}

			FORCE_INLINE MapEntry<K,V,Ctx>* getObject(inttype index) const {
				return m_items[index].m_object;
			}

			inttype instanceIndex(const Node* node) const;

			void split(TreeMap<K,V,Ctx>* self);
			void shiftLeft(inttype count);
			void splitWith(TreeMap<K,V,Ctx>* self, Branch* node, inttype index);

			void append(Item& item);
			void append(Node* node, MapEntry<K,V,Ctx>* obj);
			void appendFrom(Branch* source, inttype begin, inttype end);

			void mergeWithRight(TreeMap<K,V,Ctx>* self, Branch* rNode, inttype index);

			void balanceWithLeft(Branch* lNode, inttype index);
			void balanceWithRight(Branch* rNode, inttype index);
			void balanceWith(Branch* node, inttype index);

			void pushLeft(inttype count, Branch* lNode, inttype pIndex);
			void pushRight(inttype count, Branch* rNode, inttype pIndex);

			FORCE_INLINE inttype getMaxIndex(TreeMap<K,V,Ctx>* self) const {
				return self->m_branchMaxIndex;
			}

			FORCE_INLINE inttype getPhysicalEntries(void) const {
				return Node::m_lastIndex;
			}

			FORCE_INLINE inttype getVirtualEntries(void) const {
				return getPhysicalEntries() + 1;
			}

			FORCE_INLINE inttype getMaxPhysicalEntries(TreeMap<K,V,Ctx>* self) const {
				return self->m_branchMaxIndex;
			}

			void isFull(TreeMap<K,V,Ctx>* self, Node* node, boolean sequential);

			FORCE_INLINE inttype isFull(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex == getMaxIndex(self);
			}

			FORCE_INLINE inttype isAlmostFull(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex >= (getMaxIndex(self) - 1);
			}

			void isLow(TreeMap<K,V,Ctx>* self, Node* node);

			FORCE_INLINE inttype isLow(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex < self->m_branchLowWater;
			}

		friend class TreeMap;
		friend class Leaf;
	};

	class Leaf : public Node {

		private:
			MapEntry<K,V,Ctx>** m_objects;

		public:
			Leaf(TreeMap* self, Branch* parent, const MapEntry<K,V,Ctx>* obj);

			virtual ~Leaf(void);

			void insert(TreeMap<K,V,Ctx>* self, const MapEntry<K,V,Ctx>* obj, inttype index, boolean sequential);

			void remove(TreeMap<K,V,Ctx>* self, inttype index);
			FORCE_INLINE void removeItem(TreeMap<K,V,Ctx>* self, inttype index) {
				remove(self, index);
			}

			#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
			const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, inttype* pos = null, boolean* end = null);
			#else
			const MapEntry<K,V,Ctx>* find(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*);
			#endif
			const MapEntry<K,V,Ctx>* lower(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**);
			const MapEntry<K,V,Ctx>* higher(const TreeMap<K,V,Ctx>* self, const K, Node**, inttype*, MapEntry<K,V,Ctx>**);

			const MapEntry<K,V,Ctx>* getEntry(inttype index);
			const MapEntry<K,V,Ctx>* nextEntry(inttype index, Node** block, inttype* location);
			const MapEntry<K,V,Ctx>* previousEntry(inttype index, Node** block, inttype* location);

			FORCE_INLINE MapEntry<K,V,Ctx>* getObject(inttype index) {
				return m_objects[index];
			}

			FORCE_INLINE void setObject(inttype index, MapEntry<K,V,Ctx>* obj) {
				m_objects[index] = obj;
			}

			Leaf* firstLeaf(void);
			Leaf* lastLeaf(void);

			inttype instanceIndex(const MapEntry<K,V,Ctx>* obj) const;

			void split(TreeMap<K,V,Ctx>* self);
			void shiftLeft(inttype count);
			void splitWith(TreeMap<K,V,Ctx>* self, Leaf* rNode, inttype index);

			void append(MapEntry<K,V,Ctx>* obj);
			void appendFrom(Leaf* source, inttype begin, inttype end);

			void mergeWithRight(TreeMap<K,V,Ctx>* self, Leaf* rNode, inttype index);

			void balanceWith(Leaf* node, inttype index);
			void balanceWithLeft(Leaf* lNode, inttype index);
			void balanceWithRight(Leaf* rNode, inttype index);

			void pushLeft(inttype count, Leaf* lNode, inttype pIndex);
			void pushRight(inttype count, Leaf* rNode, inttype pIndex);

			FORCE_INLINE inttype getMaxIndex(TreeMap<K,V,Ctx>* self) const {
				return self->m_leafMaxIndex;
			}

			FORCE_INLINE inttype getPhysicalEntries(void) const {
				return Node::m_lastIndex + 1;
			}

			FORCE_INLINE inttype getVirtualEntries(void) const {
				return getPhysicalEntries() + 1;
			}

			FORCE_INLINE inttype getMaxPhysicalEntries(TreeMap<K,V,Ctx>* self) const {
				return self->m_leafMaxIndex + 1;
			}

			FORCE_INLINE inttype isFull(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex == getMaxIndex(self);
			}

			FORCE_INLINE inttype isAlmostFull(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex >= (getMaxIndex(self) -1);
			}

			FORCE_INLINE inttype isLow(TreeMap<K,V,Ctx>* self) const {
				return Node::m_lastIndex < self->m_leafLowWater;
			}

		friend class TreeMap;
		friend class Branch;
	};

	private:
		Node* m_root;
		const Comparator<K>* m_comparator;

		inttype m_pEntries;
		#ifdef COM_DEEPIS_DB_INDEX_REF
		inttype m_vEntries;
		volatile uinttype m_modification;
		#endif
		ubytetype m_leafLowWater;
		ubytetype m_leafMaxIndex;
		ubytetype m_branchLowWater;
		ubytetype m_branchMaxIndex;
		bytetype m_stateFlags;
		#ifdef COM_DEEPIS_DB_CARDINALITY
		bytetype m_keyParts;
		inttype* m_cardinality;
		#endif

		Ctx m_ctx;

		static const Comparator<K> COMPARATOR;

	private:
		#ifdef COM_DEEPIS_DB_CARDINALITY
		void initialize(bytetype order, boolean delkey, boolean delval, bytetype keyParts = -1);
		#else
		void initialize(bytetype order, boolean delkey, boolean delval);
		#endif
		void notifyRootFull(void);
		void notifyRootEmpty(void);

		FORCE_INLINE void incrementEntries(void) {
			m_pEntries++;

			#ifdef COM_DEEPIS_DB_INDEX_REF
			if (getVirtualSizeEnabled() == true) {
				m_vEntries++;
			}

			m_modification++;
			#endif
		}

		FORCE_INLINE void decrementEntries(void) {
			m_pEntries--;

			#ifdef COM_DEEPIS_DB_INDEX_REF
			if (getVirtualSizeEnabled() == true) {
				m_vEntries--;
			}

			m_modification++;
			#endif
		}

		const MapEntry<K,V,Ctx>* nextEntry(Node* node, inttype index, Node** block, inttype* location);
		const MapEntry<K,V,Ctx>* previousEntry(Node* node, inttype index, Node** block, inttype* location);

		const boolean hasNextEntry(Node* node, inttype index);
		const boolean hasPreviousEntry(Node* node, inttype index);

		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype getCardinalityPosition(K key, Node* n, inttype index) {
			Node* nn;
			inttype ii;
			inttype nextPos = 0;
			const MapEntry<K,V,Ctx>* ne = nextEntry(n, index, &nn, &ii);
			if (ne != null) {
				m_comparator->compare(key, ne->getKey(), &nextPos);
			}

			inttype prevPos = 0;
			const MapEntry<K,V,Ctx>* pe = previousEntry(n, index, &nn, &ii);
			if (pe != null) {
				m_comparator->compare(key, pe->getKey(), &prevPos);
			}

			return (prevPos > nextPos) ? prevPos : nextPos;
		}
		#endif

	public:
		FORCE_INLINE void setDeleteKey(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x04 : m_stateFlags & ~0x04;
		}

		FORCE_INLINE boolean getDeleteKey() const {
			return (m_stateFlags & 0x04) != 0;
		}

		FORCE_INLINE void setDeleteValue(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x02 : m_stateFlags & ~0x02;
		}

		FORCE_INLINE boolean getDeleteValue() const {
			return (m_stateFlags & 0x02) != 0;
		}

	public:
		FORCE_INLINE void setMapContext(Ctx ctx) {
			m_ctx = ctx;
		}

		FORCE_INLINE Ctx getMapContext() const {
			return m_ctx;
		}

	public:
		#ifdef COM_DEEPIS_DB_INDEX_REF
		FORCE_INLINE void setModification(uinttype modification) {
			m_modification = modification;
		}

		FORCE_INLINE uinttype getModification(void) const {
			return m_modification;
		}

		FORCE_INLINE void setVirtualSizeEnabled(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x01 : m_stateFlags & ~0x01;
		}

		FORCE_INLINE boolean getVirtualSizeEnabled() const {
			return (m_stateFlags & 0x01) != 0;
		}
		#endif

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE void setCardinalityEnabled(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x08 : m_stateFlags & ~0x08;
		}

		FORCE_INLINE boolean getCardinalityEnabled() const {
			return (m_stateFlags & 0x08) != 0;
		}
		#endif

		#if defined(COM_DEEPIS_DB_INDEX_REF) || defined(COM_DEEPIS_DB_CARDINALITY)
		FORCE_INLINE void setStatisticsEnabled(boolean flag) {
			#ifdef COM_DEEPIS_DB_INDEX_REF
			setVirtualSizeEnabled(flag);
			#endif

			#ifdef COM_DEEPIS_DB_CARDINALITY
			setCardinalityEnabled(flag);
			#endif
		}

		FORCE_INLINE boolean getStatisticsEnabled() const {
			boolean enabled = true;

			#ifdef COM_DEEPIS_DB_INDEX_REF
			enabled = (enabled && getVirtualSizeEnabled());
			#endif

			#ifdef COM_DEEPIS_DB_CARDINALITY
			enabled = (enabled && getCardinalityEnabled());
			#endif

			return enabled;
		}
		#endif

		// XXX: used for extreme optimization
		FORCE_INLINE void transfer(TreeMap* tree) {
			tree->m_leafLowWater = m_leafLowWater;
			tree->m_leafMaxIndex = m_leafMaxIndex;
			tree->m_branchLowWater = m_branchLowWater;
			tree->m_branchMaxIndex = m_branchMaxIndex;

			#ifdef COM_DEEPIS_DB_INDEX_REF
			if (tree->getVirtualSizeEnabled() == true) {
				tree->m_vEntries = m_vEntries;
			}

			tree->m_modification++;
			m_modification++;
			#endif

			tree->m_stateFlags = m_stateFlags;
			tree->m_pEntries = m_pEntries;
			tree->m_root = m_root;

			#ifdef COM_DEEPIS_DB_INDEX_REF
			if (getVirtualSizeEnabled() == true) {
				m_vEntries = 0;
			}
			#endif

			#ifdef COM_DEEPIS_DB_CARDINALITY
			if (getCardinalityEnabled() == true) {
				if (m_cardinality != null) {
					for (int i=0; i<m_keyParts; i++) {
						tree->m_cardinality[i] = m_cardinality[i];
					}
					memset(m_cardinality, 0, m_keyParts * sizeof(inttype));

					#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
					tree->verifyCardinality();
					#endif
				}
			}
			#endif

			m_pEntries = 0;
			m_root = null;
		}

	public:
		static const bytetype INITIAL_ORDER;

	public:

		/*
		TreeMap(const Map* map);
		*/
		TreeMap(bytetype order = INITIAL_ORDER /* XXX: purposely signed */, boolean delkey = false, boolean delval = false);
		#ifdef COM_DEEPIS_DB_CARDINALITY
		TreeMap(const Comparator<K>* comparator, bytetype order = INITIAL_ORDER, boolean delkey = false, boolean delval = false, bytetype keyParts = -1);
		#else
		TreeMap(const Comparator<K>* comparator, bytetype order = INITIAL_ORDER, boolean delkey = false, boolean delval = false);
		#endif
		virtual ~TreeMap();

		virtual const K firstKey(boolean* status) const;
		virtual const K firstKey(void) const {
			return firstKey(null);
		}

		virtual const K lastKey(boolean* status) const;
		virtual const K lastKey(void) const {
			return lastKey(null);
		}

		virtual const K lowerKey(const K key, boolean* status) const;
		virtual const K lowerKey(const K key) const {
			return lowerKey(key, null);
		}

		virtual const K higherKey(const K key, boolean* status) const;
		virtual const K higherKey(const K key) const {
			return higherKey(key, null);
		}

		virtual const K floorKey(const K key, boolean* status) const;
		virtual const K floorKey(const K key) const {
			return floorKey(key, null);
		}

		virtual const K ceilingKey(const K key, boolean* status) const;
		virtual const K ceilingKey(const K key) const {
			return ceilingKey(key, null);
		}

		virtual const MapEntry<K,V,Ctx>* firstEntry(void) const;
		virtual const MapEntry<K,V,Ctx>* lastEntry(void) const;
		virtual const MapEntry<K,V,Ctx>* getEntry(const K key) const;
		virtual const MapEntry<K,V,Ctx>* lowerEntry(const K key) const;
		virtual const MapEntry<K,V,Ctx>* higherEntry(const K key) const;
		virtual const MapEntry<K,V,Ctx>* floorEntry(const K key) const;
		virtual const MapEntry<K,V,Ctx>* ceilingEntry(const K key) const;

		#ifdef COM_DEEPIS_DB_INDEX_REF
		// XXX: following add methods are optimized for sequential access
		FORCE_INLINE void add(K key, V val, MapEntry<K,V,Ctx>** retentry = null);
		FORCE_INLINE V add(K key, V val, K* retkey, boolean* last, boolean replace, MapEntry<K,V,Ctx>** retentry = null);
		FORCE_INLINE V add(K key, V val, boolean* last, boolean replace) {
			return add(key, val, null, last, replace, null);
		}
		#endif

		FORCE_INLINE V put(K key, V val, K* retkey, boolean* status, MapEntry<K,V,Ctx>** retentry = null);
		FORCE_INLINE V put(K key, V val, K* retkey) {
			return put(key, val, retkey, null, null);
		}
		virtual V put(K key, V val) {
			return put(key, val, null, null, null);
		}

		virtual void putAll(const Map<K,V,Ctx>* map, Map<K,V,Ctx>* fillmap);
		virtual void putAll(const Map<K,V,Ctx>* map) {
			putAll(map, null);
		}

		FORCE_INLINE V remove(const K key, K* retkey, boolean* status);
		FORCE_INLINE V remove(const K key, K* retkey) {
			return remove(key, retkey, null);
		}
		virtual V remove(const K key) {
			return remove(key, null, null);
		}

		FORCE_INLINE const V get(const K key, K* retkey, boolean* status) const;
		FORCE_INLINE const V get(const K key, K* retkey) const {
			return get(key, retkey, null);
		}
		virtual const V get(const K key) const {
			return get(key, null, null);
		}

		virtual boolean containsKey(const K key) const;
		virtual boolean containsValue(const V val) const;

		virtual boolean isEmpty() const {
			return (size() == 0);
		}

		virtual inttype size() const {
			return m_pEntries;
		}

		#ifdef COM_DEEPIS_DB_INDEX_REF
		FORCE_INLINE inttype vsize(inttype nEntries) {
			inttype oEntries = m_vEntries;
			m_vEntries = nEntries;
			return oEntries;
		}

		FORCE_INLINE inttype vsize() const {
			return m_vEntries;
		}
		#endif

		#ifdef COM_DEEPIS_DB_CARDINALITY
		const inttype* getCardinality() const {
			return m_cardinality;
		}

		void recalculateCardinality() {
			if (m_cardinality != null) {
				memset(m_cardinality, 0, m_keyParts * sizeof(inttype));

				inttype pos = 0;
				K lastKey = (K) Converter<K>::NULL_VALUE;

				typename EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator iter(this);
				while (iter.EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator::hasNext()) {
					MapEntry<K,V,Ctx>* x = iter.EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator::next();

					if (lastKey != (K) Converter<K>::NULL_VALUE) {
						m_comparator->compare(lastKey, x->getKey(), &pos);
					}

					m_cardinality[pos]++;
					pos = 0;

					lastKey = x->getKey();
				}
			}
		}
		#endif

		#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
		void verifyCardinality() const {
			if (m_cardinality != null) {
				inttype total = 0;

				for (int i=0; i<m_keyParts; i++) {
					if (m_cardinality[i] < 0) {
						throw new UnsupportedOperationException("Cardinality negative");
					}
					total += m_cardinality[i];
				}

				if (total != m_vEntries) {
					throw new UnsupportedOperationException("Cardinality mismatch");
				}
			}
		}
		#endif

		virtual void clear(boolean delkey, boolean delval);
		virtual void clear() {
			clear(getDeleteKey(), getDeleteValue());
		}

		virtual Set<MapEntry<K,V,Ctx>*>* entrySet(Set<MapEntry<K,V,Ctx>*>* fillset);
		virtual Set<MapEntry<K,V,Ctx>*>* entrySet() {
			return entrySet(null);
		}

		virtual Set<K>* keySet(Set<K>* retset);
		virtual Set<K>* keySet() {
			return keySet(null);
		}

		virtual Collection<V>* values();

		virtual SortedMap<K,V,Ctx>* headMap(const K toKey, SortedMap<K,V,Ctx>* fillmap);
		virtual SortedMap<K,V,Ctx>* headMap(const K toKey) {
			return headMap(toKey, null);
		}

		virtual SortedMap<K,V,Ctx>* subMap(const K fromKey, const K toKey, SortedMap<K,V,Ctx>* fillmap);
		virtual SortedMap<K,V,Ctx>* subMap(const K fromKey, const K toKey) {
			return subMap(fromKey, toKey, null);
		}

		virtual SortedMap<K,V,Ctx>* tailMap(const K fromKey, SortedMap<K,V,Ctx>* fillmap);
		virtual SortedMap<K,V,Ctx>* tailMap(const K fromKey) {
			return tailMap(fromKey, null);
		}

		virtual TreeIterator<MapEntry<K,V,Ctx>*>* iterator(const K startKey, TreeIterator<MapEntry<K,V,Ctx>*>* filliter, boolean ceiling = true, boolean* exact = null);
		virtual TreeIterator<MapEntry<K,V,Ctx>*>* iterator(const K startKey) {
			return iterator(startKey, null);
		}

	template<typename E=MapEntry<K,V,Ctx>*>
	class TreeMapIterator : public TreeIterator<E> {

		private:
			TreeMap<K,V,Ctx>* m_map;
			Node* m_cursorNode;
			inttype m_cursorIndex;
			#ifdef COM_DEEPIS_DB_INDEX_REF
			uinttype m_modification;
			#endif

			TreeMapIterator(TreeMap<K,V,Ctx>* map, const K startKey, boolean ceiling, boolean* exact) {
				reset(map, startKey, ceiling, exact);
			}

			void reset(TreeMap<K,V,Ctx>* map, const K startKey, boolean ceiling, boolean* exact) {
				m_map = map;
				m_cursorNode = null;
				m_cursorIndex = -1;

				#ifdef COM_DEEPIS_DB_INDEX_REF
				m_modification = m_map->m_modification;
				#endif

				if (m_map->m_root != null) {
					if (startKey != Map<K,V,Ctx>::NULL_KEY) {
						K key = Map<K,V,Ctx>::NULL_KEY;
						boolean status;
						if (ceiling == true) {
							key = m_map->ceilingKey(startKey, &status);

						} else {
							key = m_map->floorKey(startKey, &status);
						}

						if (status == true) {
							m_map->m_root->find(m_map, key, &m_cursorNode, &m_cursorIndex);

						} else {
							setEnd();
						}

						if (exact != null) {
							*exact = status;
						}
					}
				}
			}

			FORCE_INLINE boolean isEnd() {
				return (m_cursorNode == null) || ((m_cursorNode == m_map->m_root) && (m_cursorIndex == (m_map->m_root->m_lastIndex + 1)));
			}

			FORCE_INLINE void setEnd() {
				m_cursorNode = m_map->m_root;
				m_cursorIndex = m_cursorNode->m_lastIndex + 1;
			}

		public:
			TreeMapIterator(void):
				m_map(null),
				m_cursorNode(null),
				#ifdef COM_DEEPIS_DB_INDEX_REF
				m_cursorIndex(-1),
				m_modification(0) {
				#else
				m_cursorIndex(-1) {
				#endif
			}

			~TreeMapIterator() {
			}

			#ifdef COM_DEEPIS_DB_INDEX_REF
			FORCE_INLINE void resetContainer(void) {
				m_modification = 0;
				m_map = null;
			}

			FORCE_INLINE uinttype getModification(void) const {
				return m_modification;
			}

			FORCE_INLINE TreeMap<K,V,Ctx>* getContainer(boolean check) {
				TreeMap<K,V,Ctx>* map = m_map;

				if ((check == true) && (map != null) && (map->m_modification != m_modification)) {
					map = null;
				}

				return map;
			}
			#endif

			inline virtual boolean hasNext() {
				return (isEnd() == false);
			}

			inline virtual boolean hasPrevious() {
				return m_map->hasPreviousEntry(m_cursorNode, m_cursorIndex);
			}

			inline virtual const E next() {
				// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
				E nextEntry = null;

				if (isEnd() == false) {
					nextEntry = (E) m_cursorNode->getEntry(m_cursorIndex);

					m_map->nextEntry(m_cursorNode, m_cursorIndex, &m_cursorNode, &m_cursorIndex);

					if (m_cursorNode == null) {
						setEnd();
					}
				}

				return nextEntry;
			}

			inline virtual const E previous() {
				E previousEntry = null;

				if (hasPrevious() == true) {
					previousEntry = (E) m_map->previousEntry(m_cursorNode, m_cursorIndex, &m_cursorNode, &m_cursorIndex);
				}

				return previousEntry;
			}

			inline virtual void remove() {
				throw new UnsupportedOperationException("Remove not supported");
			}

		friend class TreeMap;
	};

	typedef TreeMapIterator<> TreeMapEntryIterator;

	friend class TreeMapIterator<MapEntry<K,V,Ctx>*>;
	friend class EntrySetIterator;
	friend class KeySetIterator;

	class EntryMap;

	template<typename E=MapEntry<K,V,Ctx>*>
	class EntrySet : public SortedSet<E> {

		public:

		class EntrySetIterator : public Iterator<E> {

			private:
				const MapEntry<K,V,Ctx>* m_entry;
				TreeMap<K,V,Ctx>* m_map;
				Node* m_startNode;
				Node* m_endNode;
				inttype m_startIndex;
				inttype m_endIndex;

				EntrySetIterator() {
				}

				EntrySetIterator(TreeMap<K,V,Ctx>* map) {
					reset(map);
				}

				EntrySetIterator(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
					reset(map, startNode, startIndex, endNode, endIndex);
				}

				void reset(TreeMap<K,V,Ctx>* map) {
					reset(map, null, -1);
				}

				void reset(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex) {
					reset(map, null, -1, endNode, endIndex);
				}

				void reset(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
					if (startNode == null) {
						m_startNode = map->m_root;
						if (m_startNode != null) {
							m_startIndex = m_startNode->isLeaf() ? -1 : 0;
						}

					} else {
						m_startNode = startNode;
						m_startIndex = startIndex;
					}

					m_entry = null;
					m_map = map;
					m_endNode = endNode;
					m_endIndex = endIndex;
				}

				FORCE_INLINE boolean end() {
					return ((m_endNode != null) && (m_startNode == m_endNode) && (m_startIndex == m_endIndex));
				}

			public:
				~EntrySetIterator() {
				}

				inline virtual boolean hasNext() {
					if (end() == true) {
						return false;

					} else {
						return m_map->hasNextEntry(m_startNode, m_startIndex);
					}
				}

				inline virtual const E next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					if (end() == true) {
						return null;

					} else {
						m_entry = m_map->nextEntry(m_startNode, m_startIndex, &m_startNode, &m_startIndex);
						return (E) m_entry;
					}
				}

				inline virtual void remove() {
					const MapEntry<K,V,Ctx>* lowerEntry = m_map->lowerEntry(m_entry->getKey());

					m_map->remove(m_entry->getKey());
					if (lowerEntry == null) {
						reset(m_map, m_endNode, m_endIndex);

					} else {
						m_map->m_root->find(m_map, lowerEntry->getKey(), &m_startNode, &m_startIndex);
					}
				}

				void reset(const K startKey, boolean ceiling = true) {
					Node* startNode = null;
					inttype startIndex = -1;

					if (m_map->m_root != null) {
						if (startKey != Map<K,V,Ctx>::NULL_KEY) {
							K key = Map<K,V,Ctx>::NULL_KEY;
							boolean status;
							if (ceiling == true) {
								key = m_map->ceilingKey(startKey, &status);

							} else {
								key = m_map->floorKey(startKey, &status);
							}

							if (status == true) {
								m_map->m_root->find(m_map, key, &startNode, &startIndex);

							} else {
								startNode = m_map->m_root;
								startIndex = startNode->m_lastIndex + 1;
							}
						}

					} else {
						throw new UnsupportedOperationException("Failed to position iterator");
					}

					reset(m_map, startNode, startIndex, null, -1);
				}

			friend class TreeMap;
			friend class EntrySet;
			friend class EntryMap;
		};

		private:
			EntrySetIterator m_iterator;
			Node* m_startNode;
			Node* m_endNode;
			inttype m_startIndex;
			inttype m_endIndex;
			boolean m_reuse : 1;

			EntrySet() {
			}

			EntrySet(TreeMap<K,V,Ctx>* map):
				m_reuse(false) {

				reset(map);
			}

			EntrySet(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex):
				m_reuse(false) {

				reset(map, endNode, endIndex);
			}

			EntrySet(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex):
				m_reuse(false) {

				reset(map, startNode, startIndex, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map) {
				reset(map, null, -1);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex) {
				reset(map, null, -1, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
				if (startNode == null) {
					m_startNode = map->m_root;
					if (m_startNode != null) {
						m_startIndex = m_startNode->isLeaf() ? -1 : 0;
					}

				} else {
					m_startNode = startNode;
					m_startIndex = startIndex;
				}

				m_endNode = endNode;
				m_endIndex = endIndex;
				m_iterator.reset(map, startNode, startIndex, endNode, endIndex);
			}

		public:
			EntrySet(boolean reuse):
				m_reuse(reuse)  {
			}

			~EntrySet(void) {
			}

			inline virtual const E first(void) const {
				E entry = null;

				if (m_startNode != null) {
					Node* node;
					inttype index;
					entry = (E) m_iterator.m_map->nextEntry(m_startNode, m_startIndex, &node, &index);

				} else {
					entry = (E) m_iterator.m_map->firstEntry();
				}

				return entry;
			}

			inline virtual const E last(void) const {
				E entry = null;

				if (m_endNode != null) {
					entry = (E) m_endNode->getEntry(m_endIndex);

				} else {
					entry = (E) m_iterator.m_map->lastEntry();
				}

				return entry;
			}

			inline virtual boolean add(E obj) {
				throw new UnsupportedOperationException("add not supported");
			}

			inline virtual boolean contains(const E obj) const {
				throw new UnsupportedOperationException("contains not supported");
			}

			inline virtual boolean remove(const E obj) {
				throw new UnsupportedOperationException("remove not supported");
			}

			inline virtual boolean isEmpty() const {
				return (size() == 0);
			}

			inline virtual inttype size() const {
				return m_iterator.m_map->size();
			}

			inline virtual void clear() {
				throw new UnsupportedOperationException("clear not supported");
			}

			inline virtual SortedSet<E>* headSet(const E toElement) {
				throw new UnsupportedOperationException("headSet not supported");
			}

			inline virtual SortedSet<E>* subSet(const E fromElement, const E toElement) {
				throw new UnsupportedOperationException("subSet not supported");
			}

			inline virtual SortedSet<E>* tailSet(const E fromElement) {
				throw new UnsupportedOperationException("tailSet not supported");
			}

			inline virtual Iterator<E>* iterator() {
				if (m_reuse == true) {
					return &m_iterator;

				} else {
					return new EntrySetIterator(m_iterator.m_map, m_startNode, m_startIndex, m_endNode, m_endIndex);
				}
			}

			inline Iterator<E>* reset(const K startKey, boolean ceiling = true) {
				m_iterator.m_map->m_root->find(m_iterator.m_map, startKey, &m_startNode, &m_startIndex);
				reset(m_iterator.m_map, m_startNode, m_startIndex, null, -1);
				return &m_iterator;
			}

			inline Iterator<E>* reset() {
				reset(m_iterator.m_map, null, -1);
				return &m_iterator;
			}

		friend class TreeMap;
	};

	typedef EntrySet<> TreeMapEntrySet;

	class KeySet : public SortedSet<K> {

		class KeySetIterator : public Iterator<K> {

			private:
				const MapEntry<K,V,Ctx>* m_entry;
				TreeMap<K,V,Ctx>* m_map;
				Node* m_startNode;
				Node* m_endNode;
				inttype m_startIndex;
				inttype m_endIndex;

				KeySetIterator() {
				}

				KeySetIterator(TreeMap<K,V,Ctx>* map) {
					reset(map);
				}

				KeySetIterator(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
					reset(map, startNode, startIndex, endNode, endIndex);
				}

				void reset(TreeMap<K,V,Ctx>* map) {
					reset(map, null, -1);
				}

				void reset(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex) {
					reset(map, null, -1, endNode, endIndex);
				}

				void reset(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
					if (startNode == null) {
						m_startNode = map->m_root;
						if (m_startNode != null) {
							m_startIndex = m_startNode->isLeaf() ? -1 : 0;
						}

					} else {
						m_startNode = startNode;
						m_startIndex = startIndex;
					}

					m_entry = null;
					m_map = map;
					m_endNode = endNode;
					m_endIndex = endIndex;
				}

				FORCE_INLINE boolean end() {
					return ((m_endNode != null) && (m_startNode == m_endNode) && (m_startIndex == m_endIndex));
				}

			public:
				inline virtual ~KeySetIterator() {
				}

				inline virtual boolean hasNext() {
					if (end() == true) {
						return false;

					} else {
						return m_map->hasNextEntry(m_startNode, m_startIndex);
					}
				}

				inline virtual const K next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					if (end() == true) {
						return Map<K,V,Ctx>::NULL_KEY;

					} else {
						m_entry = m_map->nextEntry(m_startNode, m_startIndex, &m_startNode, &m_startIndex);
						return m_entry->getKey();
					}
				}

				inline virtual void remove() {
					const MapEntry<K,V,Ctx>* lowerEntry = m_map->lowerEntry(m_entry->getKey());

					m_map->remove(m_entry->getKey());
					if (lowerEntry == null) {
						reset(m_map, m_endNode, m_endIndex);

					} else {
						m_map->m_root->find(m_map, lowerEntry->getKey(), &m_startNode, &m_startIndex);
					}
				}

			friend class KeySet;
			friend class TreeMap;
		};

		private:
			KeySetIterator m_iterator;
			Node* m_startNode;
			Node* m_endNode;
			inttype m_startIndex;
			inttype m_endIndex;
			boolean m_reuse : 1;

			KeySet() {
			}

			KeySet(TreeMap<K,V,Ctx>* map):
				m_reuse(false) {
				reset(map);
			}

			KeySet(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex):
				m_reuse(false) {
				reset(map, endNode, endIndex);
			}

			KeySet(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex):
				m_reuse(false) {
				reset(map, startNode, startIndex, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map) {
				reset(map, null, -1);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex) {
				reset(map, null, -1, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
				if (startNode == null) {
					m_startNode = map->m_root;
					if (m_startNode != null) {
						m_startIndex = m_startNode->isLeaf() ? -1 : 0;
					}

				} else {
					m_startNode = startNode;
					m_startIndex = startIndex;
				}

				m_endNode = endNode;
				m_endIndex = endIndex;
				m_iterator.reset(map, startNode, startIndex, endNode, endIndex);
			}

		public:
			KeySet(boolean reuse):
				m_reuse(reuse)  {
			}

			inline virtual ~KeySet(void) {
			}

			inline virtual const K first(void) const {
				K key = Map<K,V,Ctx>::NULL_KEY;

				if (m_startNode != null) {
					Node* node;
					inttype index;
					const MapEntry<K,V,Ctx>* entry = m_iterator.m_map->nextEntry(m_startNode, m_startIndex, &node, &index);
					if (entry != null) {
						key = entry->getKey();
					}

				} else {
					key = m_iterator.m_map->firstKey();
				}

				return key;
			}

			inline virtual const K last(void) const {
				K key = Map<K,V,Ctx>::NULL_KEY;

				if (m_endNode != null) {
					key = m_endNode->getEntry(m_endIndex)->getKey();

				} else {
					key = m_iterator.m_map->lastKey();
				}

				return key;
			}

			inline virtual boolean add(K obj) {
				throw new UnsupportedOperationException("add not supported");
			}

			inline virtual boolean contains(const K key) const {
				boolean status = false;

				inttype firstCmp = TreeMap<K,V,Ctx>::COMPARATOR.compare(key, first());
				inttype lastCmp = TreeMap<K,V,Ctx>::COMPARATOR.compare(key, last());
				if ((firstCmp >= 0) && (lastCmp <= 0)) {
					status = m_iterator.m_map->containsKey(key);
				}

				return status;
			}

			inline virtual boolean remove(const K obj) {
				throw new UnsupportedOperationException("remove not supported");
			}

			inline virtual boolean isEmpty() const {
				return (size() == 0);
			}

			inline virtual inttype size() const {
				throw new UnsupportedOperationException("size not supported");
			}

			inline virtual void clear() {
				throw new UnsupportedOperationException("clear not supported");
			}

			inline virtual SortedSet<K>* headSet(const K toElement) {
				throw new UnsupportedOperationException("headSet not supported");
			}

			inline virtual SortedSet<K>* subSet(const K fromElement, const K toElement) {
				throw new UnsupportedOperationException("subSet not supported");
			}

			inline virtual SortedSet<K>* tailSet(const K fromElement) {
				throw new UnsupportedOperationException("tailSet not supported");
			}

			inline virtual Iterator<K>* iterator() {
				if (m_reuse == true) {
					return &m_iterator;

				} else {
					return new KeySetIterator(m_iterator.m_map, m_startNode, m_startIndex, m_endNode, m_endIndex);
				}
			}

			inline Iterator<K>* reset() {
				reset(m_iterator.m_map, null, -1);
				return &m_iterator;
			}

			friend class TreeMap;
			friend class EntryMap;
	};

	class EntryMap : public SortedMap<K,V,Ctx> {

		private:
			Node* m_startNode;
			Node* m_endNode;
			KeySet m_keySet;
			TreeMapEntrySet m_entrySet;
			inttype m_startIndex;
			inttype m_endIndex;
			boolean m_reuse : 1;

			EntryMap();

			EntryMap(TreeMap<K,V,Ctx>* map):
				m_keySet((boolean) false),
				m_entrySet((boolean) false),
				m_reuse(false) {

				reset(map);
			}

			EntryMap(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex):
				m_keySet((boolean) false),
				m_entrySet((boolean) false),
				m_reuse(false) {

				reset(map, endNode, endIndex);
			}

			EntryMap(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex):
				m_keySet((boolean) false),
				m_entrySet((boolean) false),
				m_reuse(false) {

				reset(map, startNode, startIndex, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map) {
				reset(map, null, -1);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* endNode, inttype endIndex) {
				reset(map, null, -1, endNode, endIndex);
			}

			void reset(TreeMap<K,V,Ctx>* map, Node* startNode, inttype startIndex, Node* endNode, inttype endIndex) {
				if (startNode == null) {
					m_startNode = map->m_root;
					if (m_startNode != null) {
						m_startIndex = m_startNode->isLeaf() ? -1 : 0;
					}

				} else {
					m_startNode = startNode;
					m_startIndex = startIndex;
				}

				m_endNode = endNode;
				m_endIndex = endIndex;
				m_entrySet.reset(map, startNode, startIndex, endNode, endIndex);
				m_keySet.reset(map, startNode, startIndex, endNode, endIndex);
			}

		public:
			EntryMap(boolean reuse):
				m_keySet(reuse),
				m_entrySet(reuse),
				m_reuse(reuse) {
			}

			~EntryMap(void) {
			}

			const K firstKey(void) const {
				return m_keySet.first();
			}

			const K lastKey(void) const {
				return m_keySet.last();
			}

			V put(K key, V val) {
				throw new UnsupportedOperationException("put not supported");
			}

			void putAll(const Map<K,V,Ctx>* map) {
				throw new UnsupportedOperationException("putAll not supported");
			}

			V remove(const K key) {
				throw new UnsupportedOperationException("remove not supported");
			}

			const V get(const K key) const {
				if (containsKey(key) == true) {
					return m_keySet.m_iterator.m_map->get(key);

				} else {
					return Map<K,V,Ctx>::NULL_VALUE;
				}
			}

			boolean containsKey(const K key) const {
				return m_keySet.contains(key);
			}

			boolean containsValue(const V val) const {
				throw new UnsupportedOperationException("containsValue not supported");
			}

			boolean isEmpty() const {
				return m_keySet.isEmpty();
			}

			inttype size() const {
				return m_keySet.size();
			}

			void clear() {
				throw new UnsupportedOperationException("clear not supported");
			}

			Set<MapEntry<K,V,Ctx>*>* entrySet() {
				if (m_reuse == true) {
					return &m_entrySet;

				} else {
					return new TreeMapEntrySet(m_entrySet.m_iterator.m_map, m_startNode, m_startIndex, m_endNode, m_endIndex);
				}
			}

			Set<K>* keySet() {
				if (m_reuse == true) {
					return &m_keySet;

				} else {
					return new KeySet(m_keySet.m_iterator.m_map, m_startNode, m_startIndex, m_endNode, m_endIndex);
				}
			}

			Collection<V>* values() {
				throw new UnsupportedOperationException("values not supported");
			}

			SortedMap<K,V,Ctx>* headMap(const K toKey) {
				throw new UnsupportedOperationException("headMap not supported");
			}

			SortedMap<K,V,Ctx>* subMap(const K fromKey, const K toKey) {
				throw new UnsupportedOperationException("subMap not supported");
			}

			SortedMap<K,V,Ctx>* tailMap(const K fromKey) {
				throw new UnsupportedOperationException("tailMap not supported");
			}

		friend class TreeMap;
	};
};

} } // namespace

#endif /*CXX_UTIL_TREEMAP_H_*/
