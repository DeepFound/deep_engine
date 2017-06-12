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
#ifndef CXX_UTIL_TREEMAP_CXX_
#define CXX_UTIL_TREEMAP_CXX_

#include <stdio.h>

#include "cxx/util/TreeMap.h"
#include "cxx/lang/UnsupportedOperationException.h"

using namespace cxx::util;

template<typename K, typename V, typename Ctx>
const bytetype TreeMap<K,V,Ctx>::INITIAL_ORDER = 3;

template<typename K, typename V, typename Ctx>
const Comparator<K> TreeMap<K,V,Ctx>::COMPARATOR;

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::TreeMap(bytetype order, boolean delkey, boolean delval) :
	m_comparator(&TreeMap<K,V,Ctx>::COMPARATOR) {

	initialize(order, delkey, delval);
}

template<typename K, typename V, typename Ctx>
#ifdef COM_DEEPIS_DB_CARDINALITY
TreeMap<K,V,Ctx>::TreeMap(const Comparator<K>* comparator, bytetype order, boolean delkey, boolean delval, bytetype keyParts) :
#else
TreeMap<K,V,Ctx>::TreeMap(const Comparator<K>* comparator, bytetype order, boolean delkey, boolean delval) :
#endif
	m_comparator(comparator) {

	#ifdef COM_DEEPIS_DB_CARDINALITY
	initialize(order, delkey, delval, keyParts);
	#else
	initialize(order, delkey, delval);
	#endif
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::~TreeMap() {
	clear();

	#ifdef COM_DEEPIS_DB_CARDINALITY
	if (m_cardinality != null) {
		free(m_cardinality);
	}
	#endif
}

template<typename K, typename V, typename Ctx>
#ifdef COM_DEEPIS_DB_CARDINALITY
void TreeMap<K,V,Ctx>::initialize(bytetype order, boolean delkey, boolean delval, bytetype keyParts) {
#else
void TreeMap<K,V,Ctx>::initialize(bytetype order, boolean delkey, boolean delval) {
#endif
	m_ctx = Converter<Ctx>::NULL_VALUE;

	if (order < INITIAL_ORDER) {
		order = INITIAL_ORDER;
	}

	m_branchMaxIndex = order;
	m_branchLowWater = (order - 1) / 2;
	m_leafMaxIndex = (2 * (order + 1)) - 1;
	m_leafLowWater = ((m_leafMaxIndex + 1) - 1) / 2 - 1;

	m_pEntries = 0;
	#ifdef COM_DEEPIS_DB_INDEX_REF
	m_vEntries = 0;
	m_modification = 0;
	#endif
	m_root = null;

	m_stateFlags = 0;

	setDeleteKey(delkey);
	setDeleteValue(delval);

	#ifdef COM_DEEPIS_DB_CARDINALITY
	// turn off cardinality statistics if not needed (i.e. only needed for composite keys)
	m_keyParts = keyParts;
	if (m_keyParts < 2) {
		m_cardinality = null;

	} else {
		m_cardinality = (inttype*) malloc(m_keyParts * sizeof(inttype));
		memset(m_cardinality, 0, m_keyParts * sizeof(inttype));
	}
	#endif
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::notifyRootFull(void) {
	Node* oldroot = m_root;
	m_root = new Branch(this, null, oldroot);
	oldroot->split(this);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::notifyRootEmpty(void) {
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

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::firstKey(boolean* status) const {
	const MapEntry<K,V,Ctx>* x = firstEntry();

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::lastKey(boolean* status) const {
	const MapEntry<K,V,Ctx>* x = lastEntry();

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::lowerKey(const K key, boolean* status) const {
	const MapEntry<K,V,Ctx>* x = lowerEntry(key);

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::higherKey(const K key, boolean* status) const {
	const MapEntry<K,V,Ctx>* x = higherEntry(key);

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::floorKey(const K key, boolean* status) const {
	const MapEntry<K,V,Ctx>* x = floorEntry(key);

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const K TreeMap<K,V,Ctx>::ceilingKey(const K key, boolean* status) const {
	const MapEntry<K,V,Ctx>* x = ceilingEntry(key);

	if (status != null) {
		*status = (x != null);
	}

	return (x ? x->getKey() : Map<K,V,Ctx>::NULL_KEY);
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::firstEntry(void) const {
	if (m_root != null) {
		return m_root->firstLeaf()->getObject(0);

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::lastEntry(void) const {
	if (m_root != null) {
		Node* node = m_root->lastLeaf();
		return ((Leaf*) node)->getObject(node->m_lastIndex);

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::getEntry(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		return m_root->find(this, key, &n, &index);

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::lowerEntry(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		MapEntry<K,V,Ctx>* next = null;
		return m_root->lower(this, key, &n, &index, &next);

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::higherEntry(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		MapEntry<K,V,Ctx>* prev = null;
		return m_root->higher(this, key, &n, &index, &prev);

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::floorEntry(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		MapEntry<K,V,Ctx>* next = null;
		const MapEntry<K,V,Ctx>* x = m_root->lower(this, key, &n, &index, &next);
		if (next != null) {
			if (m_comparator->compare(next->getKey(), key) == 0) {
				return next;
			}
		}

		return x;

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::ceilingEntry(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		MapEntry<K,V,Ctx>* prev = null;
		const MapEntry<K,V,Ctx>* x = m_root->higher(this, key, &n, &index, &prev);
		if (prev != null) {
			if (m_comparator->compare(prev->getKey(), key) == 0) {
				return prev;
			}
		}

		return x;

	} else {
		return null;
	}
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::nextEntry(Node* node, inttype index, Node** block, inttype* location) {
	const MapEntry<K,V,Ctx>* entry = node->nextEntry(index, block, location);
	if (entry == null) {
		while (node->m_parent != null) {
			inttype nextIndex = node->m_parent->instanceIndex(node) + 1;
			entry = node->m_parent->getEntry(nextIndex);
			if (entry != null) {
				*block = node->m_parent;
				*location = nextIndex;
				break;
			}

			node = node->m_parent;
		}
	}

	return entry;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::previousEntry(Node* node, inttype index, Node** block, inttype* location) {
	const MapEntry<K,V,Ctx>* entry = node->previousEntry(index, block, location);
	if (entry == null) {
		while (node->m_parent != null) {
			inttype previousIndex = node->m_parent->instanceIndex(node);
			entry = node->m_parent->getEntry(previousIndex);
			if (entry != null) {
				*block = node->m_parent;
				*location = previousIndex;
				break;
			}

			node = node->m_parent;
		}
	}

	return entry;
}

template<typename K, typename V, typename Ctx>
const boolean TreeMap<K,V,Ctx>::hasNextEntry(Node* node, inttype index) {
	boolean hasNext = false;

	if (node != null) {
		inttype location;
		Node* block;
		const MapEntry<K,V,Ctx>* entry = node->nextEntry(index, &block, &location);
		if (entry == null) {
			while (node->m_parent != null) {
				inttype nextIndex = node->m_parent->instanceIndex(node) + 1;
				entry = node->m_parent->getEntry(nextIndex);
				if (entry != null) {
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

template<typename K, typename V, typename Ctx>
const boolean TreeMap<K,V,Ctx>::hasPreviousEntry(Node* node, inttype index) {
	boolean hasPrevious = false;

	if (node != null) {
		inttype location;
		Node* block;
		const MapEntry<K,V,Ctx>* entry = node->previousEntry(index, &block, &location);
		if (entry == null) {
			while (node->m_parent != null) {
				inttype previousIndex = node->m_parent->instanceIndex(node);
				entry = node->m_parent->getEntry(previousIndex);
				if (entry != null) {
					hasPrevious = true;
					break;
				}

				node = node->m_parent;
			}

		} else {
			hasPrevious = true;
		}
	}

	return hasPrevious;
}

#ifdef COM_DEEPIS_DB_INDEX_REF
template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::add(K key, V val, MapEntry<K,V,Ctx>** retentry) {
	MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
	if (m_root != null) {
		Node* node = m_root->lastLeaf();

		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype pos = 0;
		if (getCardinalityEnabled() == true) {
			const MapEntry<K,V,Ctx>* x = node->getEntry(node->m_lastIndex);
			m_comparator->compare(key, x->getKey(), &pos);
		}
		#endif

		node->insert(this, p, node->m_lastIndex + 1, true);

		#ifdef COM_DEEPIS_DB_CARDINALITY
		if (getCardinalityEnabled() == true) {
			if (m_cardinality != null) {
				m_cardinality[pos]++;

				#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
				Node* nnn;
				inttype iii;
				m_root->find(this, key, &nnn, &iii);
				if (getCardinalityPosition(key, nnn, iii) != pos) {
					throw new UnsupportedOperationException("Cardinality bad position");
				}

				verifyCardinality();
				#endif
			}
		}
		#endif

	} else {
		m_root = new Leaf(this, null, p);
		incrementEntries();

		#ifdef COM_DEEPIS_DB_CARDINALITY
		if (getCardinalityEnabled() == true) {
			if (m_cardinality != null) {
				m_cardinality[0]++;

				#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
				verifyCardinality();
				#endif
			}
		}
		#endif
	}
	if (retentry != null) {
		*retentry = p;
	}
}

template<typename K, typename V, typename Ctx>
V TreeMap<K,V,Ctx>::add(K key, V val, K* retkey, boolean* last, boolean replace, MapEntry<K,V,Ctx>** retentry) {
	V retval = Map<K,V,Ctx>::NULL_VALUE;

	if (m_root != null) {
		Node* n;
		inttype index;
		inttype pos = 0;
		*last = false;
		MapEntry<K,V,Ctx>* x = (MapEntry<K,V,Ctx>*) m_root->find(this, key, &n, &index, &pos, last);
		if (x != null) {
			retval = x->getValue();

			if (retkey != null) {
				*retkey = x->getKey();
			}

			if (replace == true) {
				x->setKey(key, getMapContext());
				x->setValue(val, getMapContext());
			}

			if (retentry != null) {
				*retentry = x;
			}

		} else {
			MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
			n->insert(this, p, index, *last);

			if (retentry != null) {
				*retentry = p;
			}

			#ifdef COM_DEEPIS_DB_CARDINALITY
			if (getCardinalityEnabled() == true) {
				if (m_cardinality != null) {
					m_cardinality[pos]++;

					#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
					Node* nnn;
					inttype iii;
					m_root->find(this, key, &nnn, &iii);
					if (getCardinalityPosition(key, nnn, iii) != pos) {
						throw new UnsupportedOperationException("Cardinality bad position");
					}

					verifyCardinality();
					#endif
				}

				*last = false;
			}
			#endif
		}

	} else {
		MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
		m_root = new Leaf(this, null, p);
		incrementEntries();

		if (retentry != null) {
			*retentry = p;
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		if (getCardinalityEnabled() == true) {
			if (m_cardinality != null) {
				m_cardinality[0]++;

				#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
				verifyCardinality();
				#endif
			}

			*last = false;

		} else {
			*last = true;
		}
		#else
		*last = true;
		#endif
	}

	return retval;
}
#endif

template<typename K, typename V, typename Ctx>
V TreeMap<K,V,Ctx>::put(K key, V val, K* retkey, boolean* status, MapEntry<K,V,Ctx>** retentry) {
	V retval = Map<K,V,Ctx>::NULL_VALUE;

	if (m_root != null) {
		Node* n;
		inttype index;
		#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
		inttype pos = 0;
		MapEntry<K,V,Ctx>* x = (MapEntry<K,V,Ctx>*) m_root->find(this, key, &n, &index, &pos);
		#else
		MapEntry<K,V,Ctx>* x = (MapEntry<K,V,Ctx>*) m_root->find(this, key, &n, &index);
		#endif
		if (x != null) {
			retval = x->getValue();

			if (retkey != null) {
				*retkey = x->getKey();

			} else if ((getDeleteKey() == true) && (key != x->getKey())) {
				Converter<K>::destroy(x->getKey());
			}

			x->setKey(key, getMapContext());
			x->setValue(val, getMapContext());

			if (status != null) {
				*status = true;
			}

			if (retentry != null) {
				*retentry = x;
			}

			#ifdef COM_DEEPIS_DB_INDEX_REF
			m_modification++;
			#endif

		} else {
			MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
			n->insert(this, p, index, false);

			if (status != null) {
				*status = false;
			}

			if (retentry != null) {
				*retentry = p;
			}

			#ifdef COM_DEEPIS_DB_CARDINALITY
			if (getCardinalityEnabled() == true) {
				if (m_cardinality != null) {
					m_cardinality[pos]++;

					#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
					Node* nnn;
					inttype iii;
					m_root->find(this, key, &nnn, &iii);
					if (getCardinalityPosition(key, nnn, iii) != pos) {
						throw new UnsupportedOperationException("Cardinality bad position");
					}

					verifyCardinality();
					#endif
				}
			}
			#endif
		}

	} else {
		MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
		m_root = new Leaf(this, null, p);
		incrementEntries();

		if (status != null) {
			*status = false;
		}

		if (retentry != null) {
			*retentry = p;
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		if (getCardinalityEnabled() == true) {
			if (m_cardinality != null) {
				m_cardinality[0]++;

				#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
				verifyCardinality();
				#endif
			}
		}
		#endif
	}

	return retval;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::putAll(const Map<K,V,Ctx>* map, Map<K,V,Ctx>* fillmap) {
	EntrySet<> set(true);
	// TODO: check type
	((TreeMap<K,V,Ctx>*) map)->entrySet(&set);

	K retkey = Map<K,V,Ctx>::NULL_KEY;
	boolean status;
	Iterator<MapEntry<K,V,Ctx>* >* iter = set.iterator();
	while (iter->hasNext()) {
		MapEntry<K,V,Ctx>* entry = (MapEntry<K,V,Ctx>*) iter->next();
		if (fillmap != null) {
			V val = put(entry->getKey(), entry->getValue(), &retkey, &status);
			if (status == true) {
				fillmap->put(retkey, val);
			}

		} else {
			put(entry->getKey(), entry->getValue());
		}
	}
}

template<typename K, typename V, typename Ctx>
V TreeMap<K,V,Ctx>::remove(const K key, K* retkey, boolean* status) {
	if (m_root != null) {
		V val = Map<K,V,Ctx>::NULL_VALUE;

		Node* n;
		inttype index;
		const MapEntry<K,V,Ctx>* x = m_root->find(this, key, &n, &index);
		if (x != null) {
			val = x->getValue();

			#ifdef COM_DEEPIS_DB_CARDINALITY
			if (getCardinalityEnabled() == true) {
				if (m_cardinality != null) {
					m_cardinality[getCardinalityPosition(key, n, index)]--;
				}
			}
			#endif

			n->remove(this, index);

			#ifdef COM_DEEPIS_DB_CARDINALITY_VERIFY
			if (getCardinalityEnabled() == true) {
				verifyCardinality();
			}
			#endif

			if (size() == 0) {
				clear();
			}

			if (status != null) {
				*status = true;
			}

			K key = (K)Converter<K>::NULL_VALUE;
			if ((retkey != null) || (getDeleteKey() == true)) {
				key = x->getKey();
			}

			Converter<MapEntry<K,V,Ctx>*>::destroy(const_cast<MapEntry<K,V,Ctx>*>(x), getMapContext());

			if (retkey != null) {
				*retkey = key;

			} else if (getDeleteKey() == true) {
				Converter<K>::destroy(key);
			}

		} else {
			if (status != null) {
				*status = false;
			}
		}

		return val;

	} else {
		if (status != null) {
			*status = false;
		}

		return Map<K,V,Ctx>::NULL_VALUE;
	}
}

template<typename K, typename V, typename Ctx>
const V TreeMap<K,V,Ctx>::get(const K key, K* retkey, boolean* status) const {
	if (m_root != null) {
		V val = Map<K,V,Ctx>::NULL_VALUE;

		Node* n;
		inttype index;
		const MapEntry<K,V,Ctx>* x = m_root->find(this, key, &n, &index);
		if (x != null) {
			val = x->getValue();

			if (status != null) {
				*status = true;
			}

			if (retkey != null) {
				*retkey = x->getKey();
			}

		} else {
			if (status != null) {
				*status = false;
			}
		}

		return val;

	} else {
		if (status != null) {
			*status = false;
		}

		return Map<K,V,Ctx>::NULL_VALUE;
	}
}

template<typename K, typename V, typename Ctx>
boolean TreeMap<K,V,Ctx>::containsKey(const K key) const {
	if (m_root != null) {
		Node* n;
		inttype index;
		const MapEntry<K,V,Ctx>* x = m_root->find(this, key, &n, &index);
		return (x != null);

	} else {
		return false;
	}
}

template<typename K, typename V, typename Ctx>
boolean TreeMap<K,V,Ctx>::containsValue(const V val) const {
	// TODO
	return false;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::clear(boolean delkey, boolean delval) {

	if (m_root != null) {
		typename EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator iter(this);
		while (iter.EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator::hasNext()) {
			MapEntry<K,V,Ctx>* x = iter.EntrySet<MapEntry<K,V,Ctx>*>::EntrySetIterator::next();

			K key = (K)Converter<K>::NULL_VALUE;
			V value = (V)Converter<V>::NULL_VALUE;
			if (delkey == true) {
				key = x->getKey();
			}
			if (delval == true) {
				value = x->getValue();
			}

			Converter<MapEntry<K,V,Ctx>*>::destroy(x, getMapContext());

			if (delkey == true) {
				Converter<K>::destroy(key);
			}

			if (delval == true) {
				Converter<V>::destroy(value);
			}
		}

		#ifdef COM_DEEPIS_DB_INDEX_REF
		m_modification++;

		if (getVirtualSizeEnabled() == true) {
			m_vEntries = 0;
		}
		#endif

		m_pEntries = 0;
		delete m_root;
	}

	#ifdef COM_DEEPIS_DB_CARDINALITY
	if (getCardinalityEnabled() == true) {
		if (m_cardinality != null) {
			memset(m_cardinality, 0, m_keyParts * sizeof(inttype));
		}
	}
	#endif

	m_root = null;
}

template<typename K, typename V, typename Ctx>
Set<MapEntry<K,V,Ctx>* >* TreeMap<K,V,Ctx>::entrySet(Set<MapEntry<K,V,Ctx>* >* fillset) {
	if (fillset != null) {
		// TODO: check type
		((EntrySet<>*) fillset)->reset(this);

	} else {
		fillset = new EntrySet<>(this);
	}

	return fillset;
}

template<typename K, typename V, typename Ctx>
Set<K>* TreeMap<K,V,Ctx>::keySet(Set<K>* fillset) {
	if (fillset != null) {
		// TODO: check type
		((KeySet*) fillset)->reset(this);

	} else {
		fillset = new KeySet(this);
	}

	return fillset;
}

template<typename K, typename V, typename Ctx>
Collection<V>* TreeMap<K,V,Ctx>::values() {
	return null; // TODO
}

template<typename K, typename V, typename Ctx>
SortedMap<K,V,Ctx>* TreeMap<K,V,Ctx>::headMap(const K toKey, SortedMap<K,V,Ctx>* fillmap) {
	inttype endIndex = -1;
	Node* endNode = null;
	if (m_root != null) {
		m_root->find(this, toKey, &endNode, &endIndex);
	}

	if (fillmap != null) {
		// TODO: check type
		((EntryMap*) fillmap)->reset(this, endNode, endIndex);

	} else {
		fillmap = new EntryMap(this, endNode, endIndex);
	}

	return fillmap;
}

template<typename K, typename V, typename Ctx>
SortedMap<K,V,Ctx>* TreeMap<K,V,Ctx>::subMap(const K fromKey, const K toKey, SortedMap<K,V,Ctx>* fillmap) {
	inttype startIndex = -1;
	inttype endIndex = -1;
	Node* startNode = null;
	Node* endNode = null;
	if (m_root != null) {
		MapEntry<K,V,Ctx>* nextEntry;
		m_root->lower(this, fromKey, &startNode, &startIndex, &nextEntry);
		m_root->find(this, toKey, &endNode, &endIndex);
	}

	if (fillmap != null) {
		// TODO: check type
		((EntryMap*) fillmap)->reset(this, startNode, startIndex, endNode, endIndex);

	} else {
		fillmap = new EntryMap(this, startNode, startIndex, endNode, endIndex);
	}

	return fillmap;
}

template<typename K, typename V, typename Ctx>
SortedMap<K,V,Ctx>* TreeMap<K,V,Ctx>::tailMap(const K fromKey, SortedMap<K,V,Ctx>* fillmap) {
	inttype startIndex = -1;
	Node* startNode = null;
	if (m_root != null) {
		MapEntry<K,V,Ctx>* nextEntry;
		m_root->lower(this, fromKey, &startNode, &startIndex, &nextEntry);
	}

	if (fillmap != null) {
		// TODO: check type
		((EntryMap*) fillmap)->reset(this, startNode, startIndex, null, -1);

	} else {
		fillmap = new EntryMap(this, startNode, startIndex, null, -1);
	}

	return fillmap;
}

template<typename K, typename V, typename Ctx>
TreeIterator<MapEntry<K,V,Ctx>* >* TreeMap<K,V,Ctx>::iterator(const K startKey, TreeIterator<MapEntry<K,V,Ctx>* >* filliter, boolean ceiling, boolean* status) {
	if (filliter != null) {
		// TODO: check type
		((TreeMapIterator<>*) filliter)->reset(this, startKey, ceiling, status);

	} else {
		filliter = new TreeMapIterator<>(this, startKey, ceiling, status);
	}

	return filliter;
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Node::Node(Branch* parent, boolean isleaf):
	m_parent(parent),
	m_lastIndex(-1),
	#ifdef CXX_UTIL_TREE_SLOTTED
	m_slotIndex(-1),
	#endif
	m_isLeaf(isleaf) {
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Branch::Branch(TreeMap<K,V,Ctx>* self, Branch* parent) :
	Node(parent, false) {

	#ifdef DEEP_DEBUG
	m_items = (Item*) calloc(1, sizeof(Item) * (getMaxIndex(self) + 1));
	#else
	m_items = (Item*) malloc(sizeof(Item) * (getMaxIndex(self) + 1));
	#endif
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Branch::Branch(TreeMap<K,V,Ctx>* self, Branch* parent, Node* oldroot) :
	Node(parent, false) {

	#ifdef DEEP_DEBUG
	m_items = (Item*) calloc(1, sizeof(Item) * (getMaxIndex(self) + 1));
	#else
	m_items = (Item*) malloc(sizeof(Item) * (getMaxIndex(self) + 1));
	#endif

	append(oldroot, null);
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Branch::~Branch(void) {
	if (Node::m_lastIndex > 0) {
		delete m_items[0].m_node;
	}

	for (inttype i = 1; i <= Node::m_lastIndex; i++) {
		delete m_items[i].m_node;
	}

	free(m_items);
	#ifdef DEEP_DEBUG
	m_items = null;
	#endif
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::insert(TreeMap<K,V,Ctx>* self, const MapEntry<K,V,Ctx>* obj, inttype index, boolean sequential) {
	Leaf* leaf = getNode(index - 1)->lastLeaf();
	leaf->insert(self, obj, leaf->m_lastIndex + 1, sequential);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::insert(TreeMap<K,V,Ctx>* self, Node* node, MapEntry<K,V,Ctx>* obj, inttype index) {
	Item newitem(node, obj);

	insert(self, newitem, index);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::insertElement(Item& item, inttype index) {
	for (inttype i = Node::m_lastIndex + 1; i > index; i--) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		getItem(i).assign(i, getItem(i - 1));
		#else
		getItem(i) = getItem(i - 1);
		#endif
	}

	setItem(index, item);

	Node::m_lastIndex++;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::insertElement(Node* node, MapEntry<K,V,Ctx>* obj, inttype index) {
	Item newitem(node, obj);

	insertElement(newitem, index);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::insert(TreeMap<K,V,Ctx>* self, Item& item, inttype index) {
	insertElement(item, index);

	if (isFull(self) == true) {
		notifyParent(self);
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::appendFrom(Branch* source, inttype begin, inttype end) {
	if (begin > end) {
		return;
	}

	for (inttype i = begin; i <= end; i++) {
		setItem(++Node::m_lastIndex, source->getItem(i));
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::append(Node* node, MapEntry<K,V,Ctx>* obj) {
	setItem(++Node::m_lastIndex, obj, node);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::append(Item& item) {
	setItem(++Node::m_lastIndex, item);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::balanceWithLeft(Branch* lNode, inttype pIndex) {
	inttype newSize = (getVirtualEntries() + lNode->getPhysicalEntries()) / 2;
	inttype indexFromHere = getPhysicalEntries() - newSize;
	pushLeft(indexFromHere, lNode, pIndex);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::balanceWithRight(Branch* rNode, inttype pIndex) {
	inttype newSize = (getPhysicalEntries() + rNode->getVirtualEntries()) / 2;
	inttype indexFromHere = getPhysicalEntries() - newSize;
	pushRight(indexFromHere, rNode, pIndex);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::balanceWith(Branch* rNode, inttype pindx) {
	if (getPhysicalEntries() < rNode->getVirtualEntries()) {
		rNode->balanceWithLeft(this, pindx);

	} else {
		balanceWithRight(rNode, pindx);
	}
}

template<typename K, typename V, typename Ctx>
#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::find(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, inttype* pos, boolean* end) {
#else
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::find(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location) {
#endif

	#ifdef CXX_UTIL_TREE_BSEARCH
	register inttype last = -1;
	register inttype start = 1;
	register inttype finish = Node::m_lastIndex;
	while (start <= finish) {
		inttype mid = (start + finish) >> 1;
		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype weight = self->m_comparator->compare(getObject(mid)->getKey(), what, pos);
		#else
		inttype weight = self->m_comparator->compare(getObject(mid)->getKey(), what);
		#endif
		if (weight == 0) {
			*block = this;
			*location = mid;
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
		for (inttype i = start; i <= last; i++) {
			#ifdef COM_DEEPIS_DB_CARDINALITY
			inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what, pos);
			#else
			inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what);
			#endif
			if (weight > 0) {
				#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
				return getNode(i - 1)->find(self, what, block, location, pos, null);
				#else
				return getNode(i - 1)->find(self, what, block, location);
				#endif
			}
		}
	}
	#else
	for (inttype i = 1 ; i <= Node::m_lastIndex; i++) {
		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what, pos);
		#else
		inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what);
		#endif
		if (weight == 0) {
			*block = this;
			*location = i;
			return getObject(i);
		}

		if (weight > 0) {
			#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
			return getNode(i - 1)->find(self, what, block, location, pos, null);
			#else
			return getNode(i - 1)->find(self, what, block, location);
			#endif
		}
	}
	#endif

	#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
	return getNode(Node::m_lastIndex)->find(self, what, block, location, pos, end);
	#else
	return getNode(Node::m_lastIndex)->find(self, what, block, location);
	#endif
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::lower(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, MapEntry<K,V,Ctx>** next) {
	for (inttype i = Node::m_lastIndex; i > 0; i--) {
		inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what);
		if (weight < 0) {
			const MapEntry<K,V,Ctx>* object = getNode(i)->lower(self, what, block, location, next);
			if (object == null) {
				*block = this;
				*location = i;
				return getObject(i);

			} else {
				if ((*next == null) && (i != Node::m_lastIndex)) {
					*next = getObject(i + 1);
				}

				return object;
			}
		}
	}

	const MapEntry<K,V,Ctx>* object = getNode(0)->lower(self, what, block, location, next);
	if ((*next == null) && (Node::m_lastIndex != 0)) {
		*next = getObject(1);
	}

	return object;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::higher(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, MapEntry<K,V,Ctx>** prev) {
	for (inttype i = 1 ; i <= Node::m_lastIndex; i++) {
		inttype weight = self->m_comparator->compare(getObject(i)->getKey(), what);
		if (weight > 0) {
			const MapEntry<K,V,Ctx>* object = getNode(i - 1)->higher(self, what, block, location, prev);
			if (object == null) {
				*block = this;
				*location = i;
				return getObject(i);

			} else {
				if ((*prev == null) && (i > 1)) {
					*prev = getObject(i - 1);
				}

				return object;
			}
		}
	}

	const MapEntry<K,V,Ctx>* object = getNode(Node::m_lastIndex)->higher(self, what, block, location, prev);
	if ((*prev == null) && (Node::m_lastIndex != 0)) {
		*prev = getObject(Node::m_lastIndex);
	}

	return object;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::getEntry(inttype index) {
	if ((index > 0) && (index <= Node::m_lastIndex)) {
		return getObject(index);
	}

	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::nextEntry(inttype index, Node** block, inttype* location) {
	if (index <= Node::m_lastIndex) {
		*block = getNode(index)->firstLeaf();
		*location = 0;
		return ((Leaf*)*block)->getObject(0);
	}

	*block = null;
	*location = -1;
	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Branch::previousEntry(inttype index, Node** block, inttype* location) {
	if (index > 0) {
		*block = getNode(index-1)->lastLeaf();
		*location = ((Leaf*)*block)->m_lastIndex;
		return ((Leaf*)*block)->getObject(*location);
	}

	*block = null;
	*location = -1;
	return null;
}

template<typename K, typename V, typename Ctx>
inttype TreeMap<K,V,Ctx>::Branch::instanceIndex(const Node* node) const {
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

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::notifyParent(TreeMap<K,V,Ctx>* self) {
	if (Node::m_parent != null) {
		Node::m_parent->isFull(self, this, false);

	} else {
		self->notifyRootFull();
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::isFull(TreeMap<K,V,Ctx>* self, Node* node, boolean sequential) {
	if (node->isLeaf() == true) {
		Leaf* left = null;
		Leaf* right = null;
		Leaf* leaf = (Leaf*) node;
		inttype leafIndex = (sequential == true) ? Node::m_lastIndex : instanceIndex(leaf);
		boolean hasRight = (leafIndex < Node::m_lastIndex) && ((right = (Leaf*) getNode(leafIndex + 1)) != null);
		boolean hasLeft = (leafIndex > 0) && ((left = (Leaf*) getNode(leafIndex - 1)) != null);
		boolean rightFull = ((hasRight == true) && (right->isAlmostFull(self) == true));
		boolean leftFull = ((hasLeft == true) && (left->isAlmostFull(self) == true));

		if (rightFull == true) {
			if (leftFull == true) {
				left->splitWith(self, leaf, leafIndex);

			} else if (hasLeft == true) {
				leaf->balanceWithLeft(left, leafIndex);

			} else {
				leaf->splitWith(self, right, leafIndex + 1);
			}

		} else if (hasRight == true) {
			leaf->balanceWithRight(right, leafIndex + 1);

		} else if (leftFull == true) {
			left->splitWith(self, leaf, leafIndex);

		} else if (hasLeft == true) {
			leaf->balanceWithLeft(left, leafIndex);
		}

	} else {
		Branch* left = null;
		Branch* right = null;
		Branch* branch = (Branch*) node;
		inttype branchIndex = instanceIndex(branch);
		boolean hasRight = (branchIndex < Node::m_lastIndex) && ((right = (Branch*) getNode(branchIndex + 1)) != null);
		boolean hasLeft = (branchIndex > 0) && ((left = (Branch*) getNode(branchIndex - 1)) != null);
		boolean rightFull = ((hasRight == true) && (right->isAlmostFull(self) == true));
		boolean leftFull = ((hasLeft == true) && (left->isAlmostFull(self) == true));

		if (rightFull == true) {
			if (leftFull == true) {
				left->splitWith(self, branch, branchIndex);

			} else if (hasLeft == true) {
				branch->balanceWithLeft(left, branchIndex);

			} else {
				branch->splitWith(self, right, branchIndex + 1);
			}

		} else if (hasRight == true) {
			branch->balanceWithRight(right, branchIndex + 1);

		} else if (leftFull == true) {
			left->splitWith(self, branch, branchIndex);

		} else if (hasLeft == true) {
			branch->balanceWithLeft(left, branchIndex);
		}
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::isLow(TreeMap<K,V,Ctx>* self, Node* node) {
	if (node->isLeaf() == true) {
		Leaf* left = null;
		Leaf* right = null;
		Leaf* leaf = (Leaf*) node;
		inttype leafIndex = instanceIndex(leaf);
		boolean hasRight = (leafIndex < Node::m_lastIndex) && ((right = (Leaf*)getNode(leafIndex + 1)) != 0);
		boolean hasLeft = (leafIndex > 0) && ((left = (Leaf*) getNode(leafIndex - 1)) != 0);

		if ((hasRight == true) && (leaf->getPhysicalEntries() + right->getVirtualEntries()) >= leaf->getMaxPhysicalEntries(self)) {
			leaf->balanceWith(right, leafIndex + 1);

		} else if ((hasLeft == true) && (leaf->getVirtualEntries() + left->getPhysicalEntries()) >= leaf->getMaxPhysicalEntries(self)) {
			left->balanceWith(leaf, leafIndex);

		} else if (hasLeft == true) {
			left->mergeWithRight(self, leaf, leafIndex);

		} else if (hasRight == true) {
			leaf->mergeWithRight(self, right, leafIndex + 1);
		}

	} else {
		Branch* left = null;
		Branch* right = null;
		Branch* branch = (Branch*) node;
		inttype branchIndex = instanceIndex(branch);
		boolean hasRight = (branchIndex < Node::m_lastIndex) && ((right = (Branch*) getNode(branchIndex + 1)) != 0);
		boolean hasLeft = (branchIndex > 0) && ((left = (Branch*) getNode(branchIndex - 1)) != 0);

		if ((hasRight == true) && (branch->getPhysicalEntries() + right->getVirtualEntries()) >= branch->getMaxPhysicalEntries(self)) {
			branch->balanceWith(right, branchIndex + 1);

		} else if ((hasLeft == true) && (branch->getVirtualEntries() + left->getPhysicalEntries()) >= branch->getMaxPhysicalEntries(self)) {
			left->balanceWith(branch, branchIndex);

		} else if (hasLeft == true) {
			left->mergeWithRight(self, branch, branchIndex);

		} else if (hasRight == true) {
			branch->mergeWithRight(self, right, branchIndex + 1);
		}
	}
}

template<typename K, typename V, typename Ctx>
typename TreeMap<K,V,Ctx>::Leaf* TreeMap<K,V,Ctx>::Branch::firstLeaf(void) {
	return getNode(0)->firstLeaf();
}

template<typename K, typename V, typename Ctx>
typename TreeMap<K,V,Ctx>::Leaf* TreeMap<K,V,Ctx>::Branch::lastLeaf(void) {
	return getNode(Node::m_lastIndex)->lastLeaf();
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::mergeWithRight(TreeMap<K,V,Ctx>* self, Branch* rNode, inttype pIndex) {
	if (rNode->getPhysicalEntries() > 0) {
		rNode->pushLeft(rNode->getPhysicalEntries(), this, pIndex);
	}

	rNode->setObject(0, Node::m_parent->getObject(pIndex));

	appendFrom(rNode, 0, 0);

	Node::m_parent->removeItem(self, pIndex);

	delete rNode;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::pushLeft(inttype indexFromHere, Branch* lNode, inttype pIndex) {
	setObject(0, Node::m_parent->getObject(pIndex));

	lNode->appendFrom(this, 0, indexFromHere - 1);

	shiftLeft(indexFromHere);

	Node::m_parent->setObject(pIndex, getObject(0));
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::pushRight(inttype indexFromHere, Branch* rNode, inttype pIndex) {
	inttype begin = Node::m_lastIndex - indexFromHere + 1;
	inttype target = rNode->m_lastIndex + indexFromHere;
	inttype source = rNode->m_lastIndex;

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

	for (inttype i = Node::m_lastIndex; i >= begin; i-- ) {
		rNode->setItem(target--, getItem(i));
	}

	Node::m_parent->setObject(pIndex, rNode->getObject(0));

	Node::m_lastIndex -= indexFromHere;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::remove(TreeMap<K,V,Ctx>* self, inttype index) {
	Leaf* leaf = getNode(index)->firstLeaf();

	setObject(index, leaf->m_objects[0]);

	leaf->removeItem(self, 0);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::removeItem(TreeMap<K,V,Ctx>* self, inttype index) {
	for (inttype to = index; to < Node::m_lastIndex; to++) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		m_items[to].assign(to, m_items[to + 1]);
		#else
		m_items[to] = m_items[to + 1];
		#endif

		#ifdef DEEP_DEBUG
		m_items[to + 1].m_node = null;
		m_items[to + 1].m_object = null;
		#endif
	}

	#ifdef DEEP_DEBUG
	m_items[Node::m_lastIndex].m_node = null;
	m_items[Node::m_lastIndex].m_object = null;
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

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::shiftLeft(inttype count) {
	if (count <= 0) {
		return;
	}

	for (inttype i = count; i <= Node::m_lastIndex; i++) {
		#ifdef CXX_UTIL_TREE_SLOTTED
		getItem(i - count).assign(i - count, getItem(i));
		#else
		getItem(i - count) = getItem(i);
		#endif
	}

	Node::m_lastIndex -= count;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::split(TreeMap<K,V,Ctx>* self) {
	Branch* nNode = new Branch(self, Node::m_parent);

	Node::m_parent->append(nNode, getObject(Node::m_lastIndex));

	nNode->appendFrom(this, Node::m_lastIndex, Node::m_lastIndex);

	Node::m_lastIndex--;

	balanceWithRight(nNode, 1);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Branch::splitWith(TreeMap<K,V,Ctx>* self, Branch* rNode, inttype kIndex) {
	rNode->setObject(0, Node::m_parent->getObject(kIndex));

	inttype numberOfKeys = getPhysicalEntries() + rNode->getVirtualEntries();
	inttype newSizeOne3rd = numberOfKeys / 3;
	inttype newSizeOneHalf = (numberOfKeys - newSizeOne3rd) / 2;
	inttype newSizeOfNode = (numberOfKeys - newSizeOne3rd - newSizeOneHalf);
	inttype indexFromHere = getPhysicalEntries() - newSizeOne3rd;
	inttype indexFromNode = rNode->getVirtualEntries() - newSizeOfNode;

	Branch* nNode = new Branch(self, Node::m_parent);
	if (indexFromHere > 0) {
		nNode->append(getItem(Node::m_lastIndex));
		Node::m_parent->insertElement(nNode, getObject(Node::m_lastIndex--), kIndex);

		if (indexFromHere > 2) {
			pushRight(indexFromHere - 1, nNode, kIndex);
		}

		rNode->pushLeft(indexFromNode, nNode, kIndex + 1);

	} else {
		nNode->append(rNode->getItem(0));

		Node::m_parent->insertElement(rNode, rNode->getObject(1), kIndex + 1);

		rNode->shiftLeft(1);
		Node::m_parent->setNode(kIndex, nNode);
		rNode->pushLeft(indexFromNode - 1, nNode, kIndex + 1);
	}

	if (Node::m_parent->isFull(self) == true) {
		Node::m_parent->notifyParent(self);
	}
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Leaf::Leaf(TreeMap<K,V,Ctx>* self, Branch* parent, const MapEntry<K,V,Ctx>* obj):
	Node(parent, true) {

	inttype msize = (getMaxIndex(self) + 1) * sizeof(MapEntry<K,V,Ctx>*);
	m_objects = (MapEntry<K,V,Ctx>**) malloc(msize);
	memset(m_objects, 0, msize);

	if (obj != null) {
		m_objects[++Node::m_lastIndex] = (MapEntry<K,V,Ctx>*) obj;
	}
}

template<typename K, typename V, typename Ctx>
TreeMap<K,V,Ctx>::Leaf::~Leaf(void) {
	free(m_objects);
	#ifdef DEEP_DEBUG
	m_objects = null;
	#endif
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::insert(TreeMap<K,V,Ctx>* self, const MapEntry<K,V,Ctx>* obj, inttype index, boolean sequential) {
	for (inttype i = Node::m_lastIndex + 1; i > index ; i--) {
		m_objects[i] = m_objects[i - 1];
		#ifdef DEEP_DEBUG
		m_objects[i - 1] = null;
		#endif
	}

	m_objects[index] = (MapEntry<K,V,Ctx>*) obj;

	Node::m_lastIndex++;

	self->incrementEntries();

	if (isFull(self) == true) {
		if (Node::m_parent != null) {
			Node::m_parent->isFull(self, this, sequential);

		} else {
			self->notifyRootFull();
		}
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::appendFrom(Leaf* source, inttype begin, inttype end) {
	if (begin > end) {
		return;
	}

	for (inttype i = begin; i <= end; i++) {
		m_objects[++Node::m_lastIndex] = source->m_objects[i];
		#ifdef DEEP_DEBUG
		source->m_objects[i] = null;
		#endif
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::append(MapEntry<K,V,Ctx>* obj) {
	m_objects[++Node::m_lastIndex] = obj;
}

template<typename K, typename V, typename Ctx>
#ifdef COM_DEEPIS_DB_CARDINALITY /* || COM_DEEPIS_DB_INDEX_REF */
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::find(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, inttype* pos, boolean* end) {
#else
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::find(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location) {
#endif

	#ifdef CXX_UTIL_TREE_BSEARCH
	register inttype last = -1;
	register inttype start = 0;
	register inttype finish = Node::m_lastIndex;
	while (start <= finish) {
		inttype mid = (start + finish) >> 1;
		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype weight = self->m_comparator->compare(m_objects[mid]->getKey(), what, pos);
		#else
		inttype weight = self->m_comparator->compare(m_objects[mid]->getKey(), what);
		#endif
		if (weight == 0) {
			*block = this;
			*location = mid;
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
		for (inttype i = start; i <= last; i++) {
			#ifdef COM_DEEPIS_DB_CARDINALITY
			inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what, pos);
			#else
			inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what);
			#endif
			if (weight > 0) {
				*block = this;
				*location = i;
				return null;
			}
		}
	}
	#else
	for (inttype i = 0; i <= Node::m_lastIndex; i++) {
		#ifdef COM_DEEPIS_DB_CARDINALITY
		inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what, pos);
		#else
		inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what);
		#endif
		if (weight == 0) {
			*block = this;
			*location = i;
			return m_objects[i];
		}

		if (weight > 0) {
			*block = this;
			*location = i;
			return null;
		}
	}
	#endif

	*block = this;
	*location = Node::m_lastIndex + 1;

	#ifdef COM_DEEPIS_DB_CARDINALITY
	if (end != null) {
		*end = true;
	}
	#endif

	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::lower(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, MapEntry<K,V,Ctx>** next) {
	*next = null;
	for (inttype i = Node::m_lastIndex; i >= 0; i--) {
		inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what);
		if (weight < 0) {
			*block = this;
			*location = i;
			return m_objects[i];
		}

		*next = m_objects[i];
	}

	*block = this;
	*location = Node::m_lastIndex + 1;
	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::higher(const TreeMap<K,V,Ctx>* self, const K what, Node** block, inttype* location, MapEntry<K,V,Ctx>** prev) {
	*prev = null;
	for (inttype i = 0; i <= Node::m_lastIndex; i++) {
		inttype weight = self->m_comparator->compare(m_objects[i]->getKey(), what);
		if (weight > 0) {
			*block = this;
			*location = i;
			return m_objects[i];
		}

		*prev = m_objects[i];
	}

	*block = this;
	*location = Node::m_lastIndex + 1;
	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::getEntry(inttype index) {
	if ((index >= 0) && (index <= Node::m_lastIndex)) {
		return getObject(index);
	}

	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::nextEntry(inttype index, Node** block, inttype* location) {
	if (index < Node::m_lastIndex) {
		*block = this;
		*location = ++index;
		return m_objects[index];

	}

	*block = null;
	*location = -1;
	return null;
}

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* TreeMap<K,V,Ctx>::Leaf::previousEntry(inttype index, Node** block, inttype* location) {
	if (index > 0) {
		*block = this;
		*location = --index;
		return m_objects[index];

	}

	*block = null;
	*location = -1;
	return null;
}

template<typename K, typename V, typename Ctx>
inttype TreeMap<K,V,Ctx>::Leaf::instanceIndex(const MapEntry<K,V,Ctx>* obj) const {
	for (inttype i = 0; i <= Node::m_lastIndex; i++) {
		if (m_objects[i] == obj) {
			return i;
		}
	}

	throw new UnsupportedOperationException("reference not found");
}

template<typename K, typename V, typename Ctx>
typename TreeMap<K,V,Ctx>::Leaf* TreeMap<K,V,Ctx>::Leaf::firstLeaf(void) {
	return this;
}

template<typename K, typename V, typename Ctx>
typename TreeMap<K,V,Ctx>::Leaf* TreeMap<K,V,Ctx>::Leaf::lastLeaf (void) {
	return this;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::balanceWithLeft(Leaf* lNode, inttype pIndex) {
	inttype newSize = (getVirtualEntries() + lNode->getPhysicalEntries()) / 2;
	inttype indexFromHere = getPhysicalEntries() - newSize;
	pushLeft(indexFromHere, lNode, pIndex);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::balanceWithRight(Leaf* rNode, inttype pIndex) {
	inttype newSize = (getPhysicalEntries() + rNode->getVirtualEntries()) / 2;
	inttype indexFromHere = getPhysicalEntries() - newSize;
	pushRight(indexFromHere, rNode, pIndex);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::balanceWith(Leaf* rNode, inttype pIndex) {
	if (getPhysicalEntries() < rNode->getVirtualEntries()) {
		rNode->balanceWithLeft(this, pIndex);

	} else {
		balanceWithRight(rNode, pIndex);
	}
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::mergeWithRight(TreeMap<K,V,Ctx>* self, Leaf* rNode, inttype pIndex) {
	rNode->pushLeft(rNode->getPhysicalEntries(), this, pIndex);

	append(Node::m_parent->getObject(pIndex));

	Node::m_parent->removeItem(self, pIndex);

	delete rNode;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::pushLeft(inttype indexFromHere, Leaf* lNode, inttype pIndex) {
	lNode->append(Node::m_parent->getObject(pIndex));

	if (indexFromHere > 1) {
		lNode->appendFrom(this, 0, indexFromHere - 2);
	}

	Node::m_parent->setObject(pIndex, m_objects[indexFromHere - 1]);

	shiftLeft(indexFromHere);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::pushRight(inttype indexFromHere, Leaf* rNode, inttype pIndex) {
	inttype begin = Node::m_lastIndex - indexFromHere + 1;
	inttype target = rNode->m_lastIndex + indexFromHere;
	inttype source = rNode->m_lastIndex;

	rNode->m_lastIndex = target;

	while (source >= 0) {
		rNode->m_objects[target--] = rNode->m_objects[source--];
		#ifdef DEEP_DEBUG
		rNode->m_objects[source + 1] = null;
		#endif
	}

	rNode->m_objects[target--] = Node::m_parent->getObject(pIndex);

	for (inttype i = Node::m_lastIndex; i > begin; i--) {
		rNode->m_objects[target--] = m_objects[i];
		#ifdef DEEP_DEBUG
		m_objects[i] = null;
		#endif
	}

	Node::m_parent->setObject(pIndex, m_objects[begin]);

	Node::m_lastIndex -= indexFromHere;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::remove(TreeMap<K,V,Ctx>* self, inttype index) {
	for (inttype to = index; to < Node::m_lastIndex; to++) {
		m_objects[to] = m_objects[to + 1];
		#ifdef DEEP_DEBUG
		m_objects[to + 1] = null;
		#endif
	}

	#ifdef DEEP_DEBUG
	m_objects[Node::m_lastIndex] = null;
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

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::shiftLeft(inttype count) {
	if (count <= 0) {
		return;
	}

	for (inttype i = count; i <= Node::m_lastIndex; i++) {
		m_objects[i - count] = m_objects[i];
		#ifdef DEEP_DEBUG
		m_objects[i] = null;
		#endif
	}

	Node::m_lastIndex -= count;
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::split(TreeMap<K,V,Ctx>* self) {
	Leaf* nNode = new Leaf(self, Node::m_parent, null);

	Node::m_parent->append(nNode, m_objects[Node::m_lastIndex--]);

	balanceWithRight(nNode, 1);
}

template<typename K, typename V, typename Ctx>
void TreeMap<K,V,Ctx>::Leaf::splitWith(TreeMap<K,V,Ctx>* self, Leaf* rNode, inttype kIndex) {
	inttype numberOfKeys = getPhysicalEntries() + rNode->getVirtualEntries();
	inttype newSizeOne3rd = numberOfKeys / 3;
	inttype newSizeOneHalf = (numberOfKeys - newSizeOne3rd) / 2;
	inttype newSizeOfNode = (numberOfKeys - newSizeOne3rd - newSizeOneHalf);
	inttype indexFromHere = getPhysicalEntries() - newSizeOne3rd;
	inttype indexFromNode = rNode->getVirtualEntries() - newSizeOfNode;

	Leaf* nNode = new Leaf(self, Node::m_parent, null);

	Node::m_parent->insertElement(nNode, m_objects[Node::m_lastIndex--], kIndex);

	pushRight(indexFromHere - 1, nNode, kIndex);
	rNode->pushLeft(indexFromNode, nNode, kIndex + 1);

	if (Node::m_parent->isFull(self) == true) {
		Node::m_parent->notifyParent(self);
	}
}

#endif /*CXX_UTIL_TREEMAP_CXX_*/
