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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEEXTRA_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEEXTRA_H_

#include "cxx/lang/String.h"
#include "cxx/util/HashMap.cxx"

#include "org/w3c/dom/DomUtil.h"

#include "com/deepis/db/store/relative/util/Versions.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace org::w3c::dom;
using namespace cxx::util::concurrent::atomic;

using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

struct ExtraStatistics {

	private:
		static const int INITIAL_CAPACITY = 4;

		boolean m_totalChange;
		AtomicLong m_totalSize;
		AtomicLong m_userSpaceSize;
		boolean m_compressionQualified;

		// file index -> compression percentage
		HashMap<ushorttype,bytetype> m_compressionPercentage;

		// file index -> total key file entry count
		HashMap<ushorttype,uinttype> m_keyFragmentationTotalCount;
		// file index -> total key dead entry count
		HashMap<ushorttype,uinttype> m_keyFragmentationDeadCount;

		// file index -> total value file entry count
		HashMap<ushorttype,uinttype> m_valueFragmentationTotalCount;
		// file index -> total value dead entry count
		HashMap<ushorttype,uinttype> m_valueFragmentationDeadCount;

	public:
		ExtraStatistics(void):
			m_totalChange(false),
			m_compressionQualified(true),

			m_compressionPercentage(INITIAL_CAPACITY, false),

			m_keyFragmentationTotalCount(INITIAL_CAPACITY, false),
			m_keyFragmentationDeadCount(INITIAL_CAPACITY, false),
			m_valueFragmentationTotalCount(INITIAL_CAPACITY, false),
			m_valueFragmentationDeadCount(INITIAL_CAPACITY, false) {
		}

		FORCE_INLINE boolean getTotalChange() const {
			return m_totalChange;
		}

		FORCE_INLINE void setTotalChange(boolean totalChange) {
			m_totalChange = totalChange;
		}

		FORCE_INLINE longtype getTotalSize() const {
			return m_totalSize.get();
		}

		FORCE_INLINE void setTotalSize(longtype totalSize) {
			if (m_totalSize.get() != totalSize) {
				m_totalSize.set(totalSize);
				m_totalChange = true;
			}
		}

		FORCE_INLINE longtype addTotalSize(longtype totalSize) {
			return m_totalSize.addAndGet(totalSize);
		}

		FORCE_INLINE longtype getUserSpaceSize() const {
			return m_userSpaceSize.get();
		}

		FORCE_INLINE void setUserSpaceSize(longtype userSpaceSize) {
			m_userSpaceSize.set(userSpaceSize);
		}

		FORCE_INLINE longtype addUserSpaceSize(longtype userSpaceSize) {
			return m_userSpaceSize.addAndGet(userSpaceSize);
		}

		FORCE_INLINE boolean getCompressionQualified(ushorttype index) const {
			return m_compressionQualified;
		}

		FORCE_INLINE void setCompressionQualified(ushorttype index, boolean qualified) {
			if (m_compressionQualified == true) {
				m_compressionQualified = qualified;
			}
		}

		FORCE_INLINE bytetype getCompressionPercentage(ushorttype index) const {
			if (m_compressionPercentage.containsKey(index) == true) {
				return m_compressionPercentage.get(index);

			} else {
				return 0;
			}
		}

		FORCE_INLINE void setCompressionPercentage(ushorttype index, bytetype stat) {
			m_compressionPercentage.put(index, stat);
		}

		FORCE_INLINE uinttype getValueFragmentationTotalCount(ushorttype index) const {
			if (m_valueFragmentationTotalCount.containsKey(index) == true) {
				return m_valueFragmentationTotalCount.get(index);

			} else {
				return 0;
			}
		}

		FORCE_INLINE void setValueFragmentationTotalCount(ushorttype index, uinttype count) {
			m_valueFragmentationTotalCount.put(index, count);
		}

		FORCE_INLINE uinttype getValueFragmentationDeadCount(ushorttype index) const {
			if (m_valueFragmentationDeadCount.containsKey(index) == true) {
				return m_valueFragmentationDeadCount.get(index);

			} else {
				return 0;
			}
		}

		FORCE_INLINE void setValueFragmentationDeadCount(ushorttype index, uinttype count) {
			m_valueFragmentationDeadCount.put(index, count);
		}

		FORCE_INLINE uinttype getKeyFragmentationTotalCount(ushorttype index) const {
			if (m_keyFragmentationTotalCount.containsKey(index) == true) {
				return m_keyFragmentationTotalCount.get(index);

			} else {
				return 0;
			}
		}

		FORCE_INLINE void setKeyFragmentationTotalCount(ushorttype index, uinttype count) {
			m_keyFragmentationTotalCount.put(index, count);
		}

		FORCE_INLINE uinttype getKeyFragmentationDeadCount(ushorttype index) const {
			if (m_keyFragmentationDeadCount.containsKey(index) == true) {
				return m_keyFragmentationDeadCount.get(index);

			} else {
				return 0;
			}
		}

		FORCE_INLINE void setKeyFragmentationDeadCount(ushorttype index, uinttype count) {
			m_keyFragmentationDeadCount.put(index, count);
		}
};

struct FragmentationStatistics_v1 {
	public:
		static const String DEAD_COUNT;
		static const String TOTAL_COUNT;
		static const String FRAGMENTATION;

	public:
		static void read(org::w3c::dom::Element& fragmentElem, ExtraStatistics* extra, ushorttype fileIndex, boolean value) {
			longtype deadCount  = fragmentElem.getAttributeUint(FragmentationStatistics_v1::DEAD_COUNT);
			longtype totalCount = fragmentElem.getAttributeUint(FragmentationStatistics_v1::TOTAL_COUNT);

			if (value == true) {
				extra->setValueFragmentationDeadCount(fileIndex, deadCount);
				extra->setValueFragmentationTotalCount(fileIndex, totalCount);

			} else {
				extra->setKeyFragmentationDeadCount(fileIndex, deadCount);
				extra->setKeyFragmentationTotalCount(fileIndex, totalCount);
			}
		}

		static void write(org::w3c::dom::Document* doc, org::w3c::dom::Element& statsElem, ExtraStatistics* extra, ushorttype fileIndex, boolean value) {
			org::w3c::dom::Element fragmentElem;
			doc->createElement(FragmentationStatistics_v1::FRAGMENTATION, &fragmentElem);

			if (value == true) {
				fragmentElem.setAttributeUint(FragmentationStatistics_v1::DEAD_COUNT, extra->getValueFragmentationDeadCount(fileIndex));
				fragmentElem.setAttributeUint(FragmentationStatistics_v1::TOTAL_COUNT, extra->getValueFragmentationTotalCount(fileIndex));

			} else {
				fragmentElem.setAttributeUint(FragmentationStatistics_v1::DEAD_COUNT, extra->getKeyFragmentationDeadCount(fileIndex));
				fragmentElem.setAttributeUint(FragmentationStatistics_v1::TOTAL_COUNT, extra->getKeyFragmentationTotalCount(fileIndex));
			}

			statsElem.appendChild(&fragmentElem, &fragmentElem);
		}
};

struct CompressionStatistics_v1 {
	public:
		static const String QUALIFIED;
		static const String COMPRESSION;
		static const String PERCENTAGE;

	public:
		static void read(org::w3c::dom::Element& compressionElem, ExtraStatistics* extra, ushorttype fileIndex) {
			longtype compressionPercentage = compressionElem.getAttributeUint(CompressionStatistics_v1::PERCENTAGE);
			extra->setCompressionPercentage(fileIndex, compressionPercentage);

			boolean qualified = true;
			if (compressionElem.hasAttribute(CompressionStatistics_v1::QUALIFIED)) {
				qualified = compressionElem.getAttributeBool(CompressionStatistics_v1::QUALIFIED);
			}

			extra->setCompressionQualified(fileIndex, qualified);
		}

		static void write(org::w3c::dom::Document* doc, org::w3c::dom::Element& statsElem, ExtraStatistics* extra, ushorttype fileIndex) {
			org::w3c::dom::Element compressionElem;
			doc->createElement(CompressionStatistics_v1::COMPRESSION, &compressionElem);

			compressionElem.setAttributeUint(CompressionStatistics_v1::PERCENTAGE, extra->getCompressionPercentage(fileIndex));
			compressionElem.setAttributeBool(CompressionStatistics_v1::QUALIFIED, extra->getCompressionQualified(fileIndex));

			statsElem.appendChild(&compressionElem, &compressionElem);
		}
};

struct ValueStatistics_v1 {
	public:
		static const String VALUE_STATISTICS;
		static const String FILE_INDEX;

	public:
		static void read(org::w3c::dom::Element& statsElem, ExtraStatistics* extra, boolean compressed) {
			ushorttype fileIndex = statsElem.getAttributeUint(ValueStatistics_v1::FILE_INDEX);

			org::w3c::dom::Element elem;
			statsElem.getFirstChild(&elem);

			if (compressed == true) {
				CompressionStatistics_v1::read(elem, extra, fileIndex);

				elem.getNextSibling(&elem);
			}

			FragmentationStatistics_v1::read(elem, extra, fileIndex, true);
		}

		static void write(org::w3c::dom::Document* doc, ExtraStatistics* extra, ushorttype fileIndex, boolean compressed) {
			org::w3c::dom::Element statsElem;
			doc->createElement(ValueStatistics_v1::VALUE_STATISTICS, &statsElem);
			doc->appendChild(&statsElem, &statsElem);

			statsElem.setAttributeUint(ValueStatistics_v1::FILE_INDEX, fileIndex);

			if (compressed == true) {
				CompressionStatistics_v1::write(doc, statsElem, extra, fileIndex);
			}

			FragmentationStatistics_v1::write(doc, statsElem, extra, fileIndex, true);
		}
};

struct KeyStatistics_v1 {
	public:
		static const String KEY_STATISTICS;
		static const String MAP_KEY_NAME;

	public:
		static void read(org::w3c::dom::Element& statsElem, ExtraStatistics* extra, const char* keyName) {
			const char* attrKeyName = statsElem.getAttribute(KeyStatistics_v1::MAP_KEY_NAME);
			if (strcmp(attrKeyName, keyName) != 0) {
				return;
			}

			ushorttype fileIndex = statsElem.getAttributeUint(ValueStatistics_v1::FILE_INDEX);

			org::w3c::dom::Element elem;
			statsElem.getFirstChild(&elem);

			FragmentationStatistics_v1::read(elem, extra, fileIndex, false);
		}

		static void write(org::w3c::dom::Document* doc, ExtraStatistics* extra, ushorttype fileIndex, const char* keyName) {
			org::w3c::dom::Element statsElem;
			doc->createElement(KeyStatistics_v1::KEY_STATISTICS, &statsElem);
			doc->appendChild(&statsElem, &statsElem);

			statsElem.setAttribute(KeyStatistics_v1::MAP_KEY_NAME, keyName);
			statsElem.setAttributeUint(ValueStatistics_v1::FILE_INDEX, fileIndex);

			FragmentationStatistics_v1::write(doc, statsElem, extra, fileIndex, false);
		}
};

struct SizeStatistics_v1 {
	public:
		static const String SIZE;
		static const String TOTAL;
		static const String USER_SPACE;
		static const String TIMESTAMP;

	public:
		static void read(org::w3c::dom::Element& statsElem, ExtraStatistics* extra) {
			extra->setTotalSize(statsElem.getAttributeUlong(SizeStatistics_v1::TOTAL));
			extra->setUserSpaceSize(statsElem.getAttributeUlong(SizeStatistics_v1::USER_SPACE));
		}

		static void write(org::w3c::dom::Document* doc, ExtraStatistics* extra) {
			org::w3c::dom::Element statsElem;
			doc->createElement(SizeStatistics_v1::SIZE, &statsElem);
			doc->appendChild(&statsElem, &statsElem);

			statsElem.setAttributeLong(SizeStatistics_v1::TIMESTAMP, System::currentTimeMillis());

			statsElem.setAttributeUlong(SizeStatistics_v1::TOTAL, extra->getTotalSize());
			statsElem.setAttributeUlong(SizeStatistics_v1::USER_SPACE, extra->getUserSpaceSize());
		}
};

struct ExtraStatistics_v1 {
	public:
		static const String TERM;
		static const String VERSION;
		static const String PROTOCOL;
		static const String KEY;
		static const String SYSTEM;

	public:
		static boolean read(RandomAccessFile* xrfile, ExtraStatistics* extra, const char* keyName, boolean primary, boolean compressed) {
			org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::readFile(xrfile);
			org::w3c::dom::Element statsElem;

			// TODO: use getLastChild / getPreviousSibling optimization
			doc->getFirstChild(&statsElem);

			do {
				if (strcmp(ValueStatistics_v1::VALUE_STATISTICS, statsElem.getNodeName()) == 0) {
					if (primary == true) {
						ValueStatistics_v1::read(statsElem, extra, compressed);
					}

				} else if (strcmp(KeyStatistics_v1::KEY_STATISTICS, statsElem.getNodeName()) == 0) {
					KeyStatistics_v1::read(statsElem, extra, keyName);

				} else if (strcmp(SizeStatistics_v1::SIZE, statsElem.getNodeName()) == 0) {
					SizeStatistics_v1::read(statsElem, extra);

					extra->setTotalChange(false);
				}

			} while (statsElem.getNextSibling(&statsElem) != null);

			delete doc;

			return true;
		}

		static void updateKey(RandomAccessFile* xwfile, ExtraStatistics* extra, ushorttype fileIndex, const char* keyName) {
			org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::newDocument();

			KeyStatistics_v1::write(doc, extra, fileIndex, keyName);

			terminate(xwfile, doc);
		}

		static void updateValue(RandomAccessFile* xwfile, ExtraStatistics* extra, ushorttype fileIndex, boolean compressed) {
			org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::newDocument();

			ValueStatistics_v1::write(doc, extra, fileIndex, compressed);

			terminate(xwfile, doc);
		}

		static void updateSize(RandomAccessFile* xwfile, ExtraStatistics* extra, boolean force = false) {
			if ((extra->getTotalChange() == true) || (force == true)) {
				org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::newDocument();

				SizeStatistics_v1::write(doc, extra);

				terminate(xwfile, doc);

				if (force == false) {
					extra->setTotalChange(false);
				}
			}
		}

		static void updateLicense(RandomAccessFile* xwfile, const char* activation_key, int system, bool isChanged) {

			if(isChanged) {
				org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::newDocument();
				org::w3c::dom::Element elem;
				doc->createElement(ExtraStatistics_v1::KEY, &elem);
				doc->appendChild(&elem, &elem);
				elem.setAttribute(ExtraStatistics_v1::KEY, activation_key);
				elem.setAttributeInt(ExtraStatistics_v1::SYSTEM, system);
				terminate(xwfile, doc);
				Properties::setActivationKeyChange(false);
			}
		}

		static void version(RandomAccessFile* xwfile) {
			org::w3c::dom::Document* doc = org::w3c::dom::DomUtil::newDocument();

			org::w3c::dom::Element elem;
			doc->createElement(ExtraStatistics_v1::VERSION, &elem);
			doc->appendChild(&elem, &elem);

			elem.setAttributeLong(ExtraStatistics_v1::PROTOCOL, Versions::getProtocolVersion());

			terminate(xwfile, doc);
		}

		static longtype validate(RandomAccessFile* file) {
			static const String marker("<mark/>\n", 8);

			longtype pos = file->length();
			String str;
			nbyte buf(marker.length());
			while (pos > 0) {
				longtype seek = pos - buf.length;
				if (seek < 0) {
					seek = 0;
				}
				file->seek(seek);
				inttype count = file->read(&buf, 0, (inttype)(pos - seek));
				if (count > 0) {
					inttype prevlen = str.length();
					String read(&buf, 0, count);
					str = read+str;

					inttype match = str.indexOf(marker);
					if (match >= 0) {
						pos = pos + prevlen - str.length() + match + marker.length();
						break;
					}

					str = String(&buf, 0, count);
				}
				pos = seek;
			}

			if (pos < 0) {
				pos = 0;
			}
			return pos-1;
		}

	private:
		static void terminate(RandomAccessFile* xwfile, org::w3c::dom::Document* doc) {
			org::w3c::dom::Element termElem;
			doc->createElement(TERM, &termElem);
			doc->appendChild(&termElem, &termElem);

			xwfile->seek(xwfile->length());

			org::w3c::dom::DomUtil::writeFile(doc, xwfile);

			xwfile->syncFilePointer();

			delete doc;
		}
};

const String FragmentationStatistics_v1::DEAD_COUNT       = "dead_count";
const String FragmentationStatistics_v1::TOTAL_COUNT      = "total_count";
const String FragmentationStatistics_v1::FRAGMENTATION    = "fragmentation";

const String CompressionStatistics_v1::QUALIFIED          = "qualified";
const String CompressionStatistics_v1::COMPRESSION        = "compression";
const String CompressionStatistics_v1::PERCENTAGE         = "percentage";

const String ValueStatistics_v1::VALUE_STATISTICS         = "value_statistics";
const String ValueStatistics_v1::FILE_INDEX               = "file_index";

const String KeyStatistics_v1::MAP_KEY_NAME               = "key_name";
const String KeyStatistics_v1::KEY_STATISTICS             = "key_statistics";

const String SizeStatistics_v1::SIZE                      = "size";
const String SizeStatistics_v1::TOTAL                     = "total";
const String SizeStatistics_v1::TIMESTAMP                 = "timestamp";
const String SizeStatistics_v1::USER_SPACE                = "user_space";

const String ExtraStatistics_v1::TERM                     = "mark";
const String ExtraStatistics_v1::VERSION                  = "version";
const String ExtraStatistics_v1::PROTOCOL                 = "protocol";
const String ExtraStatistics_v1::KEY                      = "activation_key";
const String ExtraStatistics_v1::SYSTEM                   = "system_uid";

} } } } } } //namespace

#endif  // COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEEXTRA_H_
