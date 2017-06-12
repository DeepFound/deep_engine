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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEBUILDER_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEBUILDER_H_

#include "cxx/util/ArrayList.h"
#include "cxx/util/CompositeKey.h"

using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class RealTimeBuilder {

	protected:
		inttype m_offset;
		ushorttype m_unpackLength;

	public:
		RealTimeBuilder(inttype offset, ushorttype unpackLength) :
			m_offset(offset),
			m_unpackLength(unpackLength) {
		}

		FORCE_INLINE void setOffset(int offset) {
			m_offset = offset;
		}

		FORCE_INLINE boolean hasHiddenKey() const {
			return false;
		}

		FORCE_INLINE uinttype getKeyParts() const {
			return 1;
		}

		FORCE_INLINE void setUnpackLength(ushorttype unpackLength) {
			m_unpackLength = unpackLength;
		}

		FORCE_INLINE ushorttype getUnpackLength() const {
			return m_unpackLength;
		}

		FORCE_INLINE ushorttype getPackLength(const bytearray key) const {
			return m_unpackLength;
		}

		FORCE_INLINE boolean hasReservedKey() const {
			return false;
		}

		FORCE_INLINE boolean getRequiresRekey() const {
			return false;
		}

		FORCE_INLINE ulongtype maxReservedKey() const {
			throw UnsupportedOperationException("Reservation not supported in this key");
		}
};

// XXX: the following base implementation handles all primitive types
template<typename K>
class KeyBuilder : public RealTimeBuilder {
	public:
		KeyBuilder() :
			RealTimeBuilder(-1, sizeof(K)) {
		}

		FORCE_INLINE K newKey() const {
			return (K) 0;
		}

		FORCE_INLINE K cloneKey(K key) const {
			return key;
		}

		FORCE_INLINE K fillKey(const bytearray bKey, K key) const {
			return *((K*) bKey);
		}

		FORCE_INLINE K fillKey(const bytearray pkey, const bytearray value, K key) const {
			return *((K*) (value + m_offset));
		}

		FORCE_INLINE void copyKey(K in, K* out) const {
			*out = in;
		}

		FORCE_INLINE void clearKey(K key) const {
			// nothing to do
		}

		FORCE_INLINE void setIgnorePrimary(K key, boolean wildcard, boolean isEqual = true) const {
			// nothing to do
		}

		FORCE_INLINE boolean getIgnorePrimary(K key) const {
			return false;
		}

		FORCE_INLINE boolean getPrimaryKey(const K key, bytearray keyBuffer, ushorttype* keyPartOffset, ushorttype* keyPartLength) const {
			return false;
		}

		FORCE_INLINE bytearray getHiddenKey(K key) const {
			throw UnsupportedOperationException("Hidden key cannot be contained in this key");
		}

		FORCE_INLINE ulongtype getReservedKey(const K key) const {
			throw UnsupportedOperationException("Reservation not supported in this key");
		}

		FORCE_INLINE boolean isPrimitive() const {
			return true;
		}

		FORCE_INLINE boolean isEqual(const K key1, const K key2) const {
			return (key1 == key2);
		}

		FORCE_INLINE boolean isMatch(const Comparator<K>* comparator, const K key, const K matchkey) const {
			return (comparator->compare(key, matchkey) == 0);
		}

		FORCE_INLINE void setMatch(const K key, K matchkey) const {
			// nothing to do
		}

		FORCE_INLINE int toString(const K key, bytearray buffer) const {
			return sprintf(buffer, "%d", key);
		}

		FORCE_INLINE inttype range(K startKey, K endKey, inttype size, const inttype* cardinality) const {
			throw UnsupportedOperationException("Range not supported in this key");
		}
};

template<>
class KeyBuilder<nbyte*> : public RealTimeBuilder {
	public:
		KeyBuilder() :
			RealTimeBuilder(-1, -1) {
		}

		FORCE_INLINE nbyte* newKey() const {
			return new nbyte(getUnpackLength());
		}

		FORCE_INLINE nbyte* cloneKey(nbyte* key) const {
			return key->clone();
		}

		FORCE_INLINE nbyte* fillKey(const bytearray bKey, nbyte* key) const {
			memcpy((bytearray) *key, bKey, getUnpackLength());

			return key;
		}

		FORCE_INLINE nbyte* fillKey(const bytearray pkey, const bytearray value, nbyte* key) const {
			memcpy((bytearray) *key, (value + m_offset), getUnpackLength());

			return key;
		}

		FORCE_INLINE void copyKey(nbyte* in, nbyte** out) const {
			/* TODO: cannot assume caller knows length
			if ((*out)->length != in->length) {
				(*out)->realloc(in->length);
			}
			*/

			memcpy(*(*out), *in, in->length);
		}

		FORCE_INLINE void clearKey(nbyte* key) const {
			key->zero();
		}

		FORCE_INLINE void setIgnorePrimary(nbyte* key, boolean wildcard, boolean isEqual = true) const {
			// nothing to do
		}

		FORCE_INLINE boolean getIgnorePrimary(nbyte* key) const {
			return false;
		}

		FORCE_INLINE boolean getPrimaryKey(const nbyte* key, bytearray keyBuffer, ushorttype* keyPartOffset, ushorttype* keyPartLength) const {
			return false;
		}

		FORCE_INLINE bytearray getHiddenKey(nbyte* key) const {
			throw UnsupportedOperationException("Hidden key cannot be contained in this key");
		}

		FORCE_INLINE ulongtype getReservedKey(const nbyte* key) const {
			throw UnsupportedOperationException("Reservation not supported in this key");
		}

		FORCE_INLINE boolean isPrimitive() const {
			return false;
		}

		FORCE_INLINE boolean isEqual(const nbyte* key1, const nbyte* key2) const {
			if (key1->length == key2->length) {
				return (memcmp(((bytearray) *key1), ((bytearray) *key2), key1->length) == 0);

			} else {
				return false;
			}
		}

		FORCE_INLINE boolean isMatch(const Comparator<nbyte*>* comparator, const nbyte* key, const nbyte* matchkey) const {
			return (comparator->compare(key, matchkey) == 0);
		}

		FORCE_INLINE void setMatch(const nbyte* key, nbyte* matchkey) const {
			// nothing to do
		}

		FORCE_INLINE ushorttype getPackLength(const bytearray key) const {
			nbyte* tempkey = newKey();
			ushorttype length = fillKey(key, tempkey)->length;
			delete tempkey;
			return length;
		}

		FORCE_INLINE int toString(const nbyte* key, bytearray buffer) const {
			return sprintf(buffer, "%s", (bytearray) *key);
		}

		FORCE_INLINE inttype range(nbyte* startKey, nbyte* endKey, inttype size, const inttype* cardinality) const {
			throw UnsupportedOperationException("Range not supported in this key");
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEBUILDER_H_*/
