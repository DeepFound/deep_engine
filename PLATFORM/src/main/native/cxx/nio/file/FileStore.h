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
#ifndef CXX_IO_FILESTORE_H_
#define CXX_IO_FILESTORE_H_

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <mntent.h>

#include "cxx/io/IOException.h"
#include "cxx/lang/Object.h"
#include "cxx/lang/String.h"
#include "cxx/util/Comparator.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace cxx { namespace nio { namespace file {

class FileStore : public Object {
	private:
		static const char* MOUNTS;
		static const inttype BUFSIZE = 1024;

		const File m_path;

		FORCE_INLINE static const File getMountPointFor(const File& file) {
			char buf[BUFSIZE];
			struct stat stats;
			memset(&stats, 0, sizeof(stats));

			if (stat(file.getPath(), &stats) != 0) {
				throw IOException((strerror_r(errno, buf, sizeof(buf)),buf));
			}

			dev_t device = stats.st_dev;

			struct mntent mount;
			FILE* mounts = setmntent(MOUNTS, "r");
			if (mounts == null) {
				throw IOException("Unable to retrieve mount points");
			}

			int err = EINVAL;
			while (getmntent_r(mounts, &mount, buf, sizeof(buf)) != null) {
				if (stat(mount.mnt_dir, &stats) != 0) {
					err = errno;
					continue;
				}

				if (stats.st_dev == device) {
					endmntent(mounts);
					return File(mount.mnt_dir);
				}
			}

			endmntent(mounts);
			throw IOException((strerror_r(err, buf, sizeof(buf)),buf));
		}

		FORCE_INLINE void readStats(struct statvfs& stats) const {
			memset(&stats, 0, sizeof(stats));
			if (statvfs(m_path.getPath(), &stats) != 0) {
				char buf[BUFSIZE];
				throw IOException((strerror_r(errno, buf, sizeof(buf)),buf));
			}
		}

		FileStore() :
			m_path(".") {
		}

		FileStore(const File& path) :
			m_path(path) {
		}

	public:
		FileStore(const FileStore& fs) :
			m_path(fs.m_path) {
		}

		virtual ~FileStore() {
		}

		FORCE_INLINE static FileStore getFileStore(const File& file) {
			return FileStore(getMountPointFor(file));
		}

		/* Object getAttribute(String attribute); */

		FORCE_INLINE ulongtype getTotalSpace() const {
			struct statvfs stats;
			readStats(stats);
			return stats.f_blocks * stats.f_frsize;
		}

		FORCE_INLINE ulongtype getUnallocatedSpace() const {
			struct statvfs stats;
			readStats(stats);
			return stats.f_bfree * stats.f_frsize;
		}

		FORCE_INLINE ulongtype getUsableSpace() const {
			struct statvfs stats;
			readStats(stats);
			return stats.f_bavail * stats.f_frsize;
		}

		FORCE_INLINE boolean isReadOnly() const {
			struct statvfs stats;
			readStats(stats);
			return (stats.f_flag & ST_RDONLY) != 0;
		}

		FORCE_INLINE const String* name() const {
			return &m_path;
		}

		/* String* type(); */

		virtual long hashCode(void) const {
			return name()->hashCode();
		}

		virtual boolean equals(const Object* obj) const {
			return name()->equals(obj);
		}
};

const char* FileStore::MOUNTS = "/proc/mounts";

} } } // namespace

namespace cxx { namespace util {

using namespace cxx::nio::file;

template<>
class Comparator<FileStore*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const FileStore* o1, const FileStore* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const FileStore* o1, const FileStore* o2) const {
		#endif
			return o1->name()->compareTo(o2->name());
		}
};

} } // namespace

#endif /*FILESTORE_H_*/
