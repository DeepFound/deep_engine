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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILEUTIL_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILEUTIL_H_

#include "cxx/io/FileUtil.h"
#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"

using namespace com::deepis::core::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class MapFileUtil {

	public:
		static const String FILE_INFIX;
		static const String FILE_SUFFIX_IRT;
		static const String FILE_SUFFIX_ERT;
		static const String FILE_SUFFIX_LRT;
		static const String FILE_SUFFIX_VRT;
		static const String FILE_SUFFIX_TRT;
		static const String FILE_SUFFIX_SRT;
		static const String FILE_SUFFIX_XRT;

		enum FileType {
			IRT,
			LRT,
			VRT,
			TRT,
			SRT,
			XRT
		};

		static boolean move(const String& source, const String& target);

		static boolean clobber(const String& fileName, const boolean values = true, uinttype* remainingFiles = null);

		static boolean deepFilesExist(const String& fileName);

		static boolean validate(File& file, FileType type, const longtype schema, const inttype size, longtype* protocol);

		static void version(BufferedRandomAccessFile* file, const longtype schema, const inttype size);

		static void format(char* buffer, const inttype index, const boolean date, const inttype size);

		static time_t fileCreationTime(const String& fileName);

		FORCE_INLINE static boolean clobberFile(const String& filePath, const String& fileName) {
			String dname = "." + File::separator;
			{
				String sname = filePath;
				inttype index = sname.lastIndexOf(File::separator);
				if (index != -1) {
					dname = sname.substring(0, index + 1);
					sname = sname.substring(index + 1, sname.length() - 1);
				}
			}

			String sname = fileName;
			inttype index = sname.lastIndexOf(File::separator);
			if (index != -1) {
				sname = sname.substring(index + 1, sname.length() - 1);
			}

			if (FileUtil::clobber(dname + sname, false /* followSymlinks */) == false) {
				return false;
			}

			return FileUtil::clobber(fileName, false /* followSymlinks */);
		}
};


} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILEUTIL_H_*/
