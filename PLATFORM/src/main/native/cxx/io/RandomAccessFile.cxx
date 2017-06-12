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
#include <errno.h>

#include "cxx/util/Logger.h"
#include "cxx/io/RandomAccessFile.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::io;

RandomAccessFile::RandomAccessFile():
	#ifdef DEEP_USERLOCK
	// nothing to do
	#else
	m_lock(false),
	#endif
	m_handle(null),
	m_fd(null),
	m_length(-1),
	m_position(0),
	m_sync_offset(0),
	m_falloc_to(0),
	m_safe_read_limit(0),
	m_protocol(-1),
	m_fileIndex(0),
	m_fileCreationTime(0),
	m_active(false),
	m_online(false),
	m_syncable(false),
	m_readonly(false),
	m_writer(null) {

	CXX_LANG_MEMORY_DEBUG_INIT()

	m_seekTotal = 0;
	m_seekInterval = 0;
}

RandomAccessFile::RandomAccessFile(const char* name, const char* mode, boolean online):
	#ifdef DEEP_USERLOCK
	// nothing to do
	#else
	m_lock(false),
	#endif
	m_handle(null),
	m_fd(null),
	m_length(-1),
	m_position(0),
	m_sync_offset(0),
	m_falloc_to(0),
	m_safe_read_limit(0),
	m_path(name),
	m_mode(mode),
	m_protocol(-1),
	m_fileIndex(0),
	m_fileCreationTime(0),
	m_active(false),
	m_online(online),
	m_syncable(false),
	m_readonly(false),
	m_writer(null) {

	CXX_LANG_MEMORY_DEBUG_INIT()

	m_seekTotal = 0;
	m_seekInterval = 0;

	if (m_online == true) {
		attach();
	}
}

RandomAccessFile::RandomAccessFile(const File* file, const char* mode, boolean online):
	#ifdef DEEP_USERLOCK
	// nothing to do
	#else
	m_lock(false),
	#endif
	m_handle(null),
	m_fd(null),
	m_length(-1),
	m_position(0),
	m_sync_offset(0),
	m_falloc_to(0),
	m_safe_read_limit(0),
	m_path(file->getPath()),
	m_mode(mode),
	m_protocol(-1),
	m_fileIndex(0),
	m_fileCreationTime(0),
	m_active(false),
	m_online(online),
	m_syncable(false),
	m_readonly(false),
	m_writer(null) {

	CXX_LANG_MEMORY_DEBUG_INIT()

	m_seekTotal = 0;
	m_seekInterval = 0;

	if (m_online == true) {
		attach();
	}
}

RandomAccessFile::~RandomAccessFile() {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	if (m_handle != null) {
		fflush(m_handle);
	}

	delete m_fd;
	m_fd = null;

	close();

	CXX_LANG_MEMORY_DEBUG_CLEAR()
}

void RandomAccessFile::attach() {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	m_length = -1;
	m_position = 0;

	if (strcmp(m_mode, "rw") == 0) {
		if (access(m_path, F_OK) == -1) {
			m_handle = fopen(m_path, "a+b");
			if (m_handle == null) {
				DEEP_LOG(ERROR, OTHER, "File failed to open: %s %d\n", m_path.data(), errno);

				throw IOException("File descriptor exhaustion");
			}

			fclose(m_handle);
		}

		m_handle = fopen(m_path, "r+b");

	} else {
		m_handle = fopen(m_path, "rb");

		m_readonly = true;
	}

	if (m_handle == null) {
		DEEP_LOG(ERROR, OTHER, "File failed to open: %d\n", errno);

		throw IOException("File failed to open");
	}

	m_length = length();
	m_sync_offset = m_length;
	m_falloc_to = m_length;

	seek(m_length);
}

void RandomAccessFile::detach() {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	// nothing to do
}

void RandomAccessFile::prefetch(inttype size, inttype multiplier) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	longtype length = m_length;
	longtype position = m_position;

	if (position != 0) {
		fseek(m_handle, 0, SEEK_SET);
	}

	nbyte data(size);
	inttype offset = size * multiplier;
	for (longtype i = 0; i < length; i += offset) {
		if (multiplier != 1) {
			fseek(m_handle, i, SEEK_SET);
		}

		inttype r = fread(data, 1, size, m_handle);
		if (r < 0) {
			break;
		}
	}

	fseek(m_handle, position, SEEK_SET);
}

void RandomAccessFile::close() {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	FILE* handle = m_handle;
	if (handle != null) {
		m_handle = null;
		fclose(handle);
	}
}
