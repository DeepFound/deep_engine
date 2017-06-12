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
#ifndef CXX_IO_FILEDESCRIPTOR_H_
#define CXX_IO_FILEDESCRIPTOR_H_

#include <stdio.h>
#include <cerrno>

#include <fcntl.h>
#include <unistd.h>

#include "cxx/lang/Object.h"
#include "cxx/io/EOFException.h"
#include "cxx/util/Logger.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace cxx { namespace io {

class RandomAccessFile;

class FileDescriptor : public Object {

	private:
		FILE* m_handle;
		int m_fileno;
		static bool s_falloc_support;

		FORCE_INLINE FileDescriptor(FILE* handle):
			m_handle(handle),
			m_fileno(fileno(m_handle)) {
		}

	public:
		FORCE_INLINE void sync() const {
			fdatasync(m_fileno);
		}

		FORCE_INLINE void preallocate(FILE* handle, longtype offset, longtype bytes) const {
			if (s_falloc_support == true) {
				if (fallocate(m_fileno, 0, offset, bytes) != 0) {
					if (errno == EOPNOTSUPP) {
						s_falloc_support = false;

					} else {			
						LOGGING_ERROR("Invalid preallocate (fallocate): %d\n", errno);
						throw IOException();	
					}
				}
			}

			if (s_falloc_support == false) {
				int err = posix_fallocate(m_fileno, offset, bytes);
				if (err != 0) {
					LOGGING_ERROR("Invalid preallocate (posix_fallocate): %d\n", err);
					throw IOException();	
				}
			}	
		}	

		FORCE_INLINE void syncRange(longtype offset, longtype bytes) const {
			int flags = SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;

			if (sync_file_range(m_fileno, offset, bytes, flags) != 0) {
				LOGGING_ERROR("Invalid syncRange: %d\n", errno);
				throw IOException();	
			}
		}

		FORCE_INLINE boolean valid() const {
			return (ftell(m_handle) != -1);
		}

	friend class RandomAccessFile;
};

} } // namespace

#endif /*CXX_IO_FILEDESCRIPTOR_H_*/
