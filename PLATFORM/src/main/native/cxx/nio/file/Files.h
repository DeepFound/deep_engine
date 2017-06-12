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
#ifndef CXX_NIO_FILE_FILES_H_
#define CXX_NIO_FILE_FILES_H_

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include "cxx/io/IOException.h"
#include "cxx/lang/Object.h"
#include "cxx/lang/String.h"
#include "cxx/nio/file/Path.h"
#include "cxx/util/Comparator.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace cxx { namespace nio { namespace file {

class Files : public Object {
	public:
		FORCE_INLINE static Path readSymbolicLink(const String& link) {
			char buf[PATH_MAX+1];
			const inttype len = readlink(link.data(), buf, PATH_MAX);
			return String(buf, len >= 0 ? len : 0);
		}

		FORCE_INLINE static boolean createSymbolicLink(const String& link, const String& target) {
			const inttype res = symlink(target.data(), link.data());
			return res == 0;
		}

		FORCE_INLINE static boolean isSymbolicLink(const String& link) {
			struct stat stats;
			const inttype res = lstat(link.data(), &stats);
			return (res == 0) && (S_ISLNK(stats.st_mode) != 0);
		}
};

} } } // namespace

#endif /*CXX_NIO_FILE_FILES_H_*/
