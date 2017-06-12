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
#ifndef COM_DEEPIS_CORE_UTIL_FILEUTIL_H_
#define COM_DEEPIS_CORE_UTIL_FILEUTIL_H_

#include "cxx/io/File.h"
#include "cxx/nio/file/Files.h"

using namespace cxx::io;
using namespace cxx::nio::file;

namespace com { namespace deepis { namespace core { namespace util {

class FileUtil {

	public:
		static boolean mkdir(const char* directory);
		static boolean mkdir(const File* directory);
		static boolean mkdir(const String& directory);

		static boolean mkdirs(const char* directory);
		static boolean mkdirs(const File* directory);
		static boolean mkdirs(const String& directory);

		static boolean exists(const char* fileName);
		static boolean exists(const String& fileName);

		static boolean clobber(const char* fileName, const boolean followSymlinks = false);   // XXX: delete
		static boolean clobber(const File* fileObject, const boolean followSymlinks = false); // XXX: delete
		static boolean clobber(const String& fileName, const boolean followSymlinks = false); // XXX: delete

		static ArrayList<String*>* list(const char* directory);
		static ArrayList<String*>* list(const File* directory);
		static ArrayList<String*>* list(const String& directory);

		static boolean move(const char* sourceName, const char* targetName);
		static boolean move(const File* sourceFile, const File* targetFile);
		static boolean move(const String& sourceName, const String& targetName);
};

} } } } // namespace

#endif /*COM_DEEPIS_CORE_UTIL_FILEUTIL_H_*/
