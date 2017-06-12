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
#ifndef CXX_UTIL_HASHMAP_H_
#define CXX_UTIL_HASHMAP_H_

#include "cxx/util/Map.h"

namespace cxx { namespace util {

template<typename K, typename V, typename Ctx = void*>
class HashMap : public Map<K,V,Ctx> {

	private:
		inttype m_poly;
		inttype m_fill;
		inttype m_size;
		inttype m_capacity;
		inttype m_entries;
		bytetype m_stateFlags;

		MapEntry<K,V,Ctx>** m_table;
		const Converter<K>* m_converter;
		const MapEntry<K,V,Ctx>* m_reserve;

		Ctx m_ctx;

		static const Converter<K> CONVERTER;

	private:
		inline int putIndex(const K key, boolean check);
		inline int getIndex(const K key, boolean match) const;

		MapEntry<K,V,Ctx>* removeObject(int index);
		MapEntry<K,V,Ctx>* insertObject(MapEntry<K,V,Ctx>* x, int index);

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

		FORCE_INLINE void setFixed(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x01 : m_stateFlags & ~0x01;
		}

		FORCE_INLINE boolean getFixed() const {
			return (m_stateFlags & 0x01) != 0;
		}

	protected:
		virtual int resize(int minused);

	public:
		FORCE_INLINE void setMapContext(Ctx ctx) {
			m_ctx = ctx;
		}

		FORCE_INLINE Ctx getMapContext() const {
			return m_ctx;
		}

	public:
		static const int RESIZE_FACTOR;
		static const int RESIZE_LOW_WATER;
		static const int RESIZE_HIGH_WATER;

		static const int INITIAL_CAPACITY;

		/*
		HashMap(const Map* map);
		*/
		HashMap(int cap = INITIAL_CAPACITY, boolean delkey = false, boolean delval = false, boolean fixed = false);
		HashMap(const Converter<K>* converter, int cap = INITIAL_CAPACITY, boolean delkey = false, boolean delval = false, boolean fixed = false);
		virtual ~HashMap();

		virtual V put(K key, V val, K* retkey, boolean* status);
		virtual V put(K key, V val, K* retkey) {
			return put(key, val, retkey, null);
		}
		virtual V put(K key, V val) {
			return put(key, val, null, null);
		}

		virtual void putAll(const Map<K,V>* map, Map<K,V>* fillmap);
		virtual void putAll(const Map<K,V>* map) {
			putAll(map, null);
		}

		virtual V remove(const K key, K* retkey, boolean* status);
		virtual V remove(const K key, K* retkey) {
			return remove(key, retkey, null);
		}
		virtual V remove(const K key) {
			return remove(key, null, null);
		}

		virtual const V get(const K key, K* retkey, boolean* status) const;
		virtual const V get(const K key, K* retkey) const {
			return get(key, retkey, null);
		}
		virtual const V get(const K key) const {
			return get(key, null, null);
		}

		virtual boolean containsKey(const K key) const;
		virtual boolean containsValue(const V val) const;

		virtual boolean isEmpty() const;
		virtual int size() const;
		virtual int capacity() const;

		virtual void clear(boolean delkey, boolean delval);
		virtual void clear() {
			clear(getDeleteKey(), getDeleteValue());
		}

		virtual Set<MapEntry<K,V,Ctx>* >* entrySet(Set<MapEntry<K,V,Ctx>* >* fillset);
		virtual Set<MapEntry<K,V,Ctx>* >* entrySet() {
			return entrySet(null);
		}

		virtual Set<K>* keySet(Set<K>* fillset);
		virtual Set<K>* keySet() {
			return keySet(null);
		}

		virtual Collection<V>* values();

	friend class EntrySetIterator;
	friend class KeySetIterator;

	template<typename E=MapEntry<K,V,Ctx>* >
	class EntrySet : public Set<E> {

		class EntrySetIterator : public Iterator<E> {

			private:
				int m_prev;
				int m_next;
				HashMap<K,V,Ctx>* m_map;

				FORCE_INLINE EntrySetIterator():
					m_prev(0),
					m_next(0),
					m_map(null) {
				}

				FORCE_INLINE EntrySetIterator(HashMap<K,V,Ctx>* map):
					m_prev(0),
					m_next(0),
					m_map(map) {
					moveNext();
				}

				FORCE_INLINE void reset(HashMap<K,V,Ctx>* map) {
					m_prev = 0;
					m_next = 0;
					m_map = map;
					moveNext();
				}

				FORCE_INLINE void moveNext() {
					while (m_next < m_map->m_size) {
						MapEntry<K,V,Ctx>* p = m_map->m_table[m_next];
						if ((p != null) && (p != m_map->m_reserve)) {
							break;
						}

						m_next++;
					}
				}

			public:
				FORCE_INLINE virtual ~EntrySetIterator() {
				}

				FORCE_INLINE virtual boolean hasNext() {
					return (m_next < m_map->m_size);
				}

				FORCE_INLINE virtual const E next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					m_prev = m_next;
					MapEntry<K,V,Ctx>* p = m_map->m_table[m_next++];
					moveNext();
					return p;
				}

				FORCE_INLINE virtual void remove() {
					// TODO: address cursor and resize
					MapEntry<K,V,Ctx>* p = m_map->m_table[m_prev];
					m_map->remove(p->getKey());
					m_next = m_prev;
					moveNext();
				}

			friend class HashMap;
			friend class EntrySet;
		};

		private:
			boolean m_reuse;
			EntrySetIterator m_iterator;

			EntrySet();

			FORCE_INLINE EntrySet(HashMap<K,V,Ctx>* map):
				m_reuse(false),
				m_iterator(map) {
			}

			FORCE_INLINE void reset(HashMap<K,V,Ctx>* map) {
				m_iterator.reset(map);
			}

		public:
			EntrySet(boolean reuse):
				m_reuse(reuse)  {
			}

			FORCE_INLINE virtual ~EntrySet(void) {
			}

			FORCE_INLINE virtual boolean add(E obj) {
				return false;
			}

			FORCE_INLINE virtual boolean contains(const E obj) const {
				return false;
			}

			FORCE_INLINE virtual boolean remove(const E obj) {
				return false;
			}

			FORCE_INLINE virtual boolean isEmpty() const {
				return false;
			}

			FORCE_INLINE virtual int size() const {
				return 0;
			}

			FORCE_INLINE virtual void clear() {
			}

			FORCE_INLINE virtual Iterator<E>* iterator() {
				if (m_reuse == true) {
					return &m_iterator;

				} else {
					return new EntrySetIterator(m_iterator.m_map);
				}
			}

		friend class HashMap;
	};

	class KeySet : public Set<K> {

		class KeySetIterator : public Iterator<K> {

			private:
				int m_prev;
				int m_next;
				HashMap<K,V,Ctx>* m_map;

				FORCE_INLINE KeySetIterator():
					m_prev(0),
					m_next(0),
					m_map(null) {
				}

				FORCE_INLINE KeySetIterator(HashMap<K,V,Ctx>* map):
					m_prev(0),
					m_next(0),
					m_map(map) {
					moveNext();
				}

				FORCE_INLINE void reset(HashMap<K,V,Ctx>* map) {
					m_prev = 0;
					m_next = 0;
					m_map = map;
					moveNext();
				}

				FORCE_INLINE void moveNext() {
					while (m_next < m_map->m_size) {
						MapEntry<K,V,Ctx>* p = m_map->m_table[m_next];
						if ((p != null) && (p != m_map->m_reserve)) {
							break;
						}

						m_next++;
					}
				}

			public:
				FORCE_INLINE virtual ~KeySetIterator() {
				}

				FORCE_INLINE virtual boolean hasNext() {
					return (m_next < m_map->m_size);
				}

				FORCE_INLINE virtual const K next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					m_prev = m_next;
					MapEntry<K,V,Ctx>* p = m_map->m_table[m_next++];
					moveNext();
					return p->getKey();
				}

				FORCE_INLINE virtual void remove() {
					// TODO: address cursor and resize
					MapEntry<K,V,Ctx>* p = m_map->m_table[m_prev];
					m_map->remove(p->getKey());
					m_next = m_prev;
					moveNext();
				}

			friend class KeySet;
			friend class HashMap;
		};

		private:
			boolean m_reuse;
			KeySetIterator m_iterator;

			KeySet();

			FORCE_INLINE KeySet(HashMap<K,V,Ctx>* map):
				m_reuse(false),
				m_iterator(map) {
			}

			FORCE_INLINE void reset(HashMap<K,V,Ctx>* map) {
				m_iterator.reset(map);
			}

		public:
			FORCE_INLINE KeySet(boolean reuse):
				m_reuse(reuse)  {
			}

			FORCE_INLINE virtual ~KeySet(void) {
			}

			FORCE_INLINE virtual boolean add(K obj) {
				// TODO
				return false;
			}

			FORCE_INLINE virtual boolean contains(const K obj) const {
				// TODO
				return false;
			}

			FORCE_INLINE virtual boolean remove(const K obj) {
				// TODO
				return false;
			}

			FORCE_INLINE virtual boolean isEmpty() const {
				// TODO
				return false;
			}

			FORCE_INLINE virtual int size() const {
				// TODO
				return 0;
			}

			FORCE_INLINE virtual void clear() {
				// TODO
			}

			FORCE_INLINE virtual Iterator<K>* iterator() {
				if (m_reuse == true) {
					return &m_iterator;

				} else {
					return new KeySetIterator(m_iterator.m_map);
				}
			}

		friend class HashMap;
	};
};

} } // namespace

#endif /*CXX_UTIL_HASHMAP_H_*/
