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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESCHEMA_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESCHEMA_H_

#include "org/w3c/dom/DomUtil.h"

#include "com/deepis/db/store/relative/util/MapFileUtil.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
class SchemaBuilder {
	private:
		static File s_file;

	public:
		SchemaBuilder(void) {
		}
		
		FORCE_INLINE ulongtype getAutoIncrementValue() const {
			// TODO
			return 1;
		}
		
		FORCE_INLINE void setAutoIncrementValue(ulongtype autoIncrementValue) {
			// TODO
		}

		FORCE_INLINE void setDirectoryPaths(const File& dataDir, const File& indexDir) {
			// TODO
		}

		FORCE_INLINE const File& getDataDirectory() const {
			return s_file;
		}

		FORCE_INLINE const File& getIndexDirectory() const {
			return s_file;
		}

		FORCE_INLINE boolean isTemporary() const {
			// TODO
			return false;
		}
		
		FORCE_INLINE RealTime::IndexOrientation getIndexOrientation() const {
			// TODO
			return RealTime::INDEX_ORIENTATION_ROW;
		}
		
		FORCE_INLINE void setIndexOrientation(RealTime::IndexOrientation indexOrientation) {
			// TODO
		}

		FORCE_INLINE boolean getKeyCompression() const {
			// TODO
			return false;
		}

		FORCE_INLINE void setKeyCompression(boolean keyCompression) {
			// TODO
		}

		FORCE_INLINE boolean getValueCompression() const {
			// TODO
			return false;
		}

		FORCE_INLINE void setValueCompression(boolean valueCompression) {
			// TODO
		}

		FORCE_INLINE inttype getLrtIndex() const {
			return 0;
		}

		FORCE_INLINE void setLrtIndex(inttype index) {
			// TODO
		}

		FORCE_INLINE uinttype getLrtPosition() const {
			return 0;
		}

		FORCE_INLINE void setLrtPosition(uinttype position) {
			// TODO
		}
		
		FORCE_INLINE const char* getKeyName() const {
			// TODO
			return "";
		}
		
		FORCE_INLINE inttype compare(const SchemaBuilder<K>* schemaBuilder) const {
			// TODO
			return 0;
		}
};

template<typename K>
File SchemaBuilder<K>::s_file("");

template<typename K>
struct KeySchema_v1 {
	public:
		FORCE_INLINE static void read(org::w3c::dom::Element& schemaElem, SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}

		FORCE_INLINE static void write(org::w3c::dom::Document* doc, const SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}
};

template<typename K>
struct FieldSchema_v1 {
	public:
		FORCE_INLINE static void read(org::w3c::dom::Element& schemaElem, SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}

		FORCE_INLINE static void write(org::w3c::dom::Document* doc, const SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}
};

template<typename K>
struct FragmentationSchema_v1 {
	public:
		FORCE_INLINE static void read(org::w3c::dom::Element& schemaElem, SchemaBuilder<K>* schemaBuilder, boolean value) {
			// TODO
		}

		FORCE_INLINE static void write(org::w3c::dom::Document* doc, org::w3c::dom::Element& statElem, const SchemaBuilder<K>* schemaBuilder, boolean value) {
			// TODO
		}
};

template<typename K>
struct KeyStatisticSchema_v1 {
	public:
		FORCE_INLINE static void read(org::w3c::dom::Element& schemaElem, SchemaBuilder<K>* schemaBuilder, const char* keyName) {
			// TODO
		}

		FORCE_INLINE static void write(org::w3c::dom::Document* doc, const SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}
};

template<typename K>
struct ValueStatisticSchema_v1 {
	public:
		FORCE_INLINE static void read(org::w3c::dom::Element& schemaElem, SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}

		FORCE_INLINE static void write(org::w3c::dom::Document* doc, const SchemaBuilder<K>* schemaBuilder) {
			// TODO
		}
};

template<typename K>
struct RealTimeSchema_v1 {
	public:
		static longtype validate(RandomAccessFile* file) {
			// TODO
			return 0;
		}

		static SchemaBuilder<K>* read(RandomAccessFile* file, const char* keyName, boolean primary) {
			// TODO
			return null;
		}

		static void write(const SchemaBuilder<K>* schemaBuilder, RandomAccessFile* file, boolean primary) {
			// TODO
		}

		static void remove(const SchemaBuilder<K>* schemaBuilder, RandomAccessFile* file, boolean primary, inttype lrtIndex, uinttype lrtPosition) {
			// TODO
		}

		static void updateAutoIncrementValue(const SchemaBuilder<K>* schemaBuilder, RandomAccessFile* file) {
			// TODO
		}
};

} } } } } } //namespace

#endif  // COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESCHEMA_H_
