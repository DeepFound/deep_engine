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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_VERSIONS_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_VERSIONS_H_ 

#include "cxx/lang/types.h"
#include "com/deepis/db/store/relative/core/Properties.h"

using namespace com::deepis::db::store::relative::core;

#define CT_DATASTORE_PROTO_VER_1_0	10
#define CT_DATASTORE_PROTO_VER_1_1	12
#define CT_DATASTORE_PROTO_VER_1_2	13

// XXX : ACP / transactionId in LRT
#define CT_DATASTORE_PROTO_VER_1_3	14

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class Versions {

	private:
		static inttype s_buildNumber;
		static inttype s_majorVersion;
		static inttype s_minorVersion;
		static inttype s_revisionNumber;

		static longtype s_protocolVersion;
		static longtype PROTOCOL_CURRENT; /* lrt, vrt, irt data proto format definition */

	public:
		static const longtype LENGTH = 32; /* maj_4.min_4.rev_4.build_4.proto_8.schema_8 */
		static const longtype PROTOCOL_MINIMUM = CT_DATASTORE_PROTO_VER_1_2; /* lrt, vrt, irt data proto format definition */

		inline static void setBuildNumber(inttype number) {
			s_buildNumber = number;
		}

		inline static inttype getBuildNumber(void) {
			return s_buildNumber;
		}

		inline static void setMajorVersion(inttype version) {
			s_majorVersion = version;
		}

		inline static inttype getMajorVersion(void) {
			return s_majorVersion;
		}

		inline static void setMinorVersion(inttype version) {
			s_minorVersion = version;
		}

		inline static inttype getMinorVersion(void) {
			return s_minorVersion;
		}

		inline static void setRevisionNumber(inttype number) {
			s_revisionNumber = number;
		}

		inline static inttype getRevisionNumber(void) {
			return s_revisionNumber;
		}

		inline static void setProtocolVersion(longtype version) {
			s_protocolVersion = version;
		}

		inline static longtype getProtocolVersion(void) {
			if (s_protocolVersion == 0) {
				s_protocolVersion = GET_PROTOCOL_CURRENT();
			}
			return s_protocolVersion;
		}

		inline static longtype GET_PROTOCOL_CURRENT(void) {
			if (PROTOCOL_CURRENT == 0) {
				PROTOCOL_CURRENT = (Properties::getTrtFileSize() > 0) ? CT_DATASTORE_PROTO_VER_1_3 : CT_DATASTORE_PROTO_VER_1_2;
			}
			return PROTOCOL_CURRENT;
		}
};

} } } } } } // namespace

#ifdef DEEP_DEBUG
#define DEEP_VERSION_ASSERT_EQUAL(va, vb, msg) if (va != vb) throw InvalidException(msg);
#define DEEP_VERSION_ASSERT_CURRENT(va, msg) if (va != Versions::GET_PROTOCOL_CURRENT()) throw InvalidException(msg);
#else
#define DEEP_VERSION_ASSERT_EQUAL(va, vb, msg)
#define DEEP_VERSION_ASSERT_CURRENT(va, msg)
#endif

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_VERSIONS_H_*/
