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
#ifndef CXX_IO_RANDOMACCESSFILE_H_
#define CXX_IO_RANDOMACCESSFILE_H_ 

#include <stdio.h>
#include <sys/stat.h>

#include <unistd.h>
#include <dirent.h>

#include "cxx/lang/Object.h"
#include "cxx/lang/String.h"
#include "cxx/lang/nbyte.h"

#ifdef DEEP_DEBUG
#include "cxx/util/Logger.h"
#endif

#include "cxx/io/File.h"
#include "cxx/io/EOFException.h"
#include "cxx/io/FileDescriptor.h"

#ifdef DEEP_USERLOCK
	#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#else
	#include "cxx/util/concurrent/locks/ReentrantLock.h"
#endif

using namespace cxx::lang;
using namespace cxx::util::concurrent::locks;

namespace cxx { namespace io {

class RandomAccessFile : public Object {

	public:
		CXX_LANG_MEMORY_DEBUG_DECL()

	private:
		#ifdef DEEP_USERLOCK
		UserSpaceLock m_lock;
		#else
		ReentrantLock m_lock;
		#endif
		UserSpaceLock m_falloc_lock;

		FILE* m_handle;
		FileDescriptor* m_fd;

		longtype m_length;
		longtype m_position;

		longtype m_sync_offset;
		longtype m_falloc_to;
		longtype m_safe_read_limit;

		boolean m_rangeSync;
		inttype m_rangeBlock;
		boolean m_forceMaxRangeBlock;

		FORCE_INLINE longtype checkSafeReadLimit(longtype len) {
			longtype writerLen = (m_writer == null) ? -1 : m_writer->RandomAccessFile::lensync();
			longtype readLen = len;
			
			if ((writerLen > -1) && ((m_position + len) > writerLen)) {
				readLen =  writerLen - m_position;
			}
			
			if (m_safe_read_limit == writerLen) {
				return readLen;
			}

			if ((writerLen > -1) && ((m_position + len) > m_safe_read_limit)) {
				// XXX: seek will flush buffers
				RandomAccessFile::seek(m_position, -1, true /* force */);
				m_safe_read_limit = writerLen;
			}

			return readLen;
		}

		FORCE_INLINE void prealloc(boolean lock, inttype len) {
			if (lock == true) m_falloc_lock.lock();
			{
				if (m_length + len > m_falloc_to) {
					int rangeBlock = ((m_forceMaxRangeBlock == false) && (m_length < m_rangeBlock)) ? m_length : m_rangeBlock;

					m_falloc_to += rangeBlock;

					// initial prealloc to size of first write
					if (m_falloc_to == 0) {
						m_falloc_to = len;
					}			
					getFD()->preallocate(m_handle, 0, m_falloc_to);
				}
			}
			if (lock == true) m_falloc_lock.unlock();
		}

	protected:
		String m_path;
		String m_mode;
		longtype m_protocol;
		ushorttype m_fileIndex;
		time_t m_fileCreationTime;

		boolean m_active;
		boolean m_online;
		boolean m_syncable;
		boolean m_readonly;
		RandomAccessFile* m_writer;

		virtual void attach(void);
		virtual void detach(void);

		FORCE_INLINE void setRangeSync(boolean rangeSync) {
			m_rangeSync = rangeSync;
		}

		FORCE_INLINE void setRangeBlock(inttype rangeBlock) {
			m_rangeBlock = rangeBlock;
		}

		FORCE_INLINE void setForceMaxRangeBlock(boolean force) {
			m_forceMaxRangeBlock = force;
		}

		FORCE_INLINE void fdsync() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			if (m_rangeSync == true) {
				if (m_sync_offset != m_length) {
					getFD()->syncRange(m_sync_offset, m_length - m_sync_offset);
					m_sync_offset = m_length;
				}

			} else {
				getFD()->sync();
			}

			m_syncable = false;
		}

	public:
		// XXX: required for map seek statistics
		ulongtype m_seekTotal;
		ulongtype m_seekInterval;

		// XXX: "only legal" when using this class as an interface
		RandomAccessFile();

		FORCE_INLINE void lock(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_lock.lock();
		}

		FORCE_INLINE boolean tryLock(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_lock.tryLock();
		}

		FORCE_INLINE void unlock(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_lock.unlock();
		}

		// XXX: share handle for internal read/write
		FORCE_INLINE void setHandle(FILE* handle) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_handle = handle;
		}

		// XXX: share handle for external read/write
		FORCE_INLINE FILE* getHandle() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_handle;
		}

		FORCE_INLINE const char* getPath() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_path;
		}

		FORCE_INLINE const char* getMode() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_mode;
		}

		FORCE_INLINE void setProtocol(longtype protocol) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_protocol = protocol;
		}

		FORCE_INLINE longtype getProtocol(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_protocol;
		}

		FORCE_INLINE void setFileIndex(ushorttype index) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_fileIndex = index;
		}

		FORCE_INLINE ushorttype getFileIndex(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_fileIndex;
		}

		FORCE_INLINE void setFileCreationTime(time_t time) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_fileCreationTime = time;
		}

		FORCE_INLINE time_t getFileCreationTime(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_fileCreationTime;
		}

		FORCE_INLINE void setActive(boolean active) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_active = active;
		}

		FORCE_INLINE boolean getActive() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_active;
		}

		FORCE_INLINE boolean setOnline(boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			boolean result = false;

			if ((flag == false) && (m_online == true)) {
				detach();

				FILE* handle = m_handle;
				if (handle != null) {

					if ((m_readonly == false) && (m_falloc_to > m_length)) {
						// XXX: fflush is necessary for truncate to take effect reliably
						fflush(handle);
						setLength(m_length, true /* modify */);
					}

					// XXX: before offlining
					if (m_syncable == true) {
						fdsync();
					}

					delete m_fd;
					m_fd = null;

					m_handle = null;
					fclose(handle);
				}

				m_active = false;
				m_online = false;
				result = true;

			} else if ((flag == true) && (m_online == false)) {
				attach();

				m_online = true;
				result = true;
			}

			return result;
		}

		FORCE_INLINE boolean getOnline(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			#ifdef DEEP_DEBUG
			if ((m_online == true) && (m_handle == null)) {
				LOGGING_ERROR("Invalid file status: %d, %d - %s\n", m_online, m_active, m_path.data());

				throw IOException();
			}
			#endif

			return m_online;
		}

		FORCE_INLINE const FileDescriptor* getFD(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			if (m_fd == null) {
				#ifdef DEEP_DEBUG
				if ((m_online == false) || (m_handle == null)) {
					LOGGING_ERROR("Invalid file handle: %d, %d - %s\n", m_online, m_active, m_path.data());

					throw IOException();
				}
				#endif

				m_fd = new FileDescriptor(m_handle);
			}

			return m_fd;
		}
		
		inline longtype syncFilePointer() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			fflush(m_handle);
			const longtype position = ftell(m_handle);
			if (position >= 0) {
				m_position = position;
			}

			if (m_length != -1) {
				setLength(-1, false);
				setLength(length(), false);
			}

			return position;
		}

		void prefetch(inttype size, inttype multiplier);

	public:
		RandomAccessFile(const char* name, const char* mode, boolean online = true);
		RandomAccessFile(const File* file, const char* mode, boolean online = true);

		virtual ~RandomAccessFile();

		inline virtual inttype read(boolean* eof = null);
		inline virtual inttype read(nbyte* b, boolean* eof = null);
		inline virtual inttype read(nbyte* b, inttype off, inttype len, boolean* eof = null);

		inline virtual void readFully(nbyte* b, boolean* eof = null);
		inline virtual void readFully(nbyte* b, inttype off, inttype len, boolean* eof = null);
		inline virtual void readFullyRaw(nbyte* b, inttype off, inttype len, boolean* eof = null);

		inline virtual void write(inttype b);
		inline virtual void write(const nbyte* b);
		inline virtual void write(const nbyte* b, inttype off, inttype len);

		inline virtual void setLength(longtype newLength, boolean modify = true);
		inline virtual inttype skipBytes(inttype n, boolean* eof = null);

		inline virtual longtype getFilePointer();
		inline virtual void seek(longtype pos, longtype len = -1, boolean force = false);

		inline longtype length() const;
		inline longtype lensync();

		inline boolean readBoolean(boolean* eof = null);
		inline bytetype readByte(boolean* eof = null);
		inline inttype readUnsignedByte(boolean* eof = null);
		inline short readShort(boolean* eof = null);
		inline inttype readUnsignedShort(boolean* eof = null);
		inline char readChar(boolean* eof = null);
		inline inttype readInt(boolean* eof = null);
		#ifdef DEEP_VALIDATE_DATA
		inline inttype readIntRaw(boolean* eof = null);
		#endif
		inline longtype readLong(boolean* eof = null);
		inline float readFloat(boolean* eof = null);
		inline double readDouble(boolean* eof = null);

		inline String* readUTF(boolean* eof = null);
		/*
		inline String* readLine(boolean* eof = null);
		*/

		inline void writeBoolean(boolean v);
		inline void writeByte(inttype v);
		inline void writeShort(inttype v);
		inline void writeChar(inttype v);
		inline void writeInt(inttype v);
		inline void writeLong(longtype v);
		inline void writeFloat(float v);
		inline void writeDouble(double v);

		inline void writeUTF(const String* str);
		inline void writeBytes(const String* str);
		inline void writeChars(const String* str);

		virtual void close();
};

inline inttype RandomAccessFile::read(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	checkSafeReadLimit(1);

	bytetype byte;
	inttype size = fread(&byte, 1, 1, m_handle);
	if (size == 1) {
		++m_position;
		return byte & 0xff;
	} else {
		if (eof != null) {
			*eof |= feof(m_handle);
		}
		return -1; /* XXX: preserve old behavior of returning -1 on 0 bytes read independent of EOF */
	}
}

inline inttype RandomAccessFile::read(nbyte* b, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	checkSafeReadLimit(b->length);

	inttype res = read(b, 0, b->length, eof);
	return res;
}

inline inttype RandomAccessFile::read(nbyte* b, inttype off, inttype len, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	len = checkSafeReadLimit(len);

	/* TODO: make this work
	if (len > m_length) {
		len = m_length;

	} else if ((len + m_position) > m_length) {
		len = m_length - m_position;
	}
	*/

	inttype size = fread(*b + off, 1, len, m_handle);
	if (size > 0) {
		m_position += size;
		if ((size < len) && (eof != null)) {
			*eof |= feof(m_handle);
		}
		return size;

	} else {
		if (eof != null) {
			*eof |= feof(m_handle);
		}
		return -1; /* XXX: preserve old behavior of returning -1 on 0 bytes read independent of EOF */
	}
}

inline void RandomAccessFile::readFully(nbyte* b, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	checkSafeReadLimit(b->length);

	return readFully(b, 0, b->length, eof);
}

inline void RandomAccessFile::readFully(nbyte* b, inttype off, inttype len, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	checkSafeReadLimit(len);

	inttype n = 0;
	do {
		inttype count = read(b, off + n, len - n, eof);
		if ((eof != null) && (*eof == true)) {
			break;
		}

		n += count;

	} while (n < len);
}

inline void RandomAccessFile::readFullyRaw(nbyte* b, inttype off, inttype len, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	checkSafeReadLimit(len);

	inttype n = 0;
	do {
		inttype count = RandomAccessFile::read(b, off + n, len - n, eof);
		if ((eof != null) && (*eof == true)) {
			break;
		}

		n += count;

	} while (n < len);
}

inline void RandomAccessFile::write(inttype b) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	m_falloc_lock.lock(); 
	{
		if (m_rangeSync == true) {
			prealloc(false /* lock */, 1 /* len */);
		}

		#ifdef DEEP_DEBUG
		inttype count = fwrite(&b, 1, 1, m_handle);
		if (count != 1) {
			throw IOException();
		}
		#else
		fwrite(&b, 1, 1, m_handle);
		#endif
		fflush(m_handle);
		m_position++;
		if (m_length < m_position) {
			m_length = m_position;
		}
	}
	m_falloc_lock.unlock();
}

inline void RandomAccessFile::write(const nbyte* b) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write(b, 0, b->length);
}

inline void RandomAccessFile::write(const nbyte* b, inttype off, inttype len) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	m_falloc_lock.lock(); 
	{
		if (m_rangeSync == true) {
			prealloc(false /* lock */, len);
		}

		#ifdef DEEP_DEBUG
		inttype count = fwrite(*b + off, 1, len, m_handle);
		if (count != len) {
			throw IOException();
		}
		#else
		fwrite(*b + off, 1, len, m_handle);
		#endif
		fflush(m_handle);
		m_position += len;
		if (m_length < m_position) {
			m_length = m_position;
		}
	}
	m_falloc_lock.unlock();
}

inline void RandomAccessFile::setLength(longtype newLength, boolean modify) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	if (modify == true) {
		inttype rc = ftruncate(fileno(m_handle), newLength);
		if (rc != 0) {
			throw IOException();
		}
		m_falloc_to = newLength;
	}

	m_length = newLength;
}

// TODO: support 64 e.g. fsetpos
inline inttype RandomAccessFile::skipBytes(inttype n, boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	if (n > 0) {
		longtype total = length();
		longtype pointer = getFilePointer();
		if ((pointer + n) > total) {
			n = (total - pointer);
			if (eof != null) {
				*eof = true;
			}
		}
		fseek(m_handle, n, SEEK_CUR);
		//m_seekInterval++;
		m_position += n;
		return n;

	} else {
		return 0;
	}
}

inline longtype RandomAccessFile::getFilePointer() {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	return m_position;
}

// TODO: support 64 e.g. fsetpos
inline void RandomAccessFile::seek(longtype pos, longtype len, boolean force) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);

	if ((force == true) || (m_position != pos)) {
		// TODO: gnulib bug workaround ( fseeko: fix bug on glibc )
		if (m_readonly == true) {
			fflush(m_handle);
		}

		fseek(m_handle, pos, SEEK_SET);
		m_seekInterval++;
		m_position = pos;
	}

	if (len != -1) {
		m_length = len;
	}
}

inline longtype RandomAccessFile::length() const {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	if (m_length == -1) {
		#if 0
		if (m_online == true) {
			longtype b_pos = ftell(m_handle);
			fseek(m_handle, 0, SEEK_END);
			longtype a_pos = ftell(m_handle);
			fseek(m_handle, b_pos, SEEK_CUR);
			m_seekInterval++;
			return a_pos;

		} else {
		#endif
			struct stat st;
			stat(m_path.data(), &st);
			return st.st_size;
		#if 0
		}
		#endif

	} else {
		return m_length;
	}
}

inline longtype RandomAccessFile::lensync() {
	if (m_length == -1) {
		m_length = length();
	}

	return m_length;
}

inline boolean RandomAccessFile::readBoolean(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);

	return (b1 != 0);
}

inline bytetype RandomAccessFile::readByte(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);

	return (bytetype) b1;
}

inline inttype RandomAccessFile::readUnsignedByte(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);

	return b1;
}

inline short RandomAccessFile::readShort(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);
	inttype b2 = read(eof);

	return (short) ((b1 << 8) + (b2 << 0));
}

inline inttype RandomAccessFile::readUnsignedShort(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);
	inttype b2 = read(eof);

	return (b1 << 8) + (b2 << 0);
}

inline char RandomAccessFile::readChar(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);
	inttype b2 = read(eof);

	return (char) ((b1 << 8) + (b2 << 0));
}

inline inttype RandomAccessFile::readInt(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = read(eof);
	inttype b2 = read(eof);
	inttype b3 = read(eof);
	inttype b4 = read(eof);

	return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
}

#ifdef DEEP_VALIDATE_DATA
inline inttype RandomAccessFile::readIntRaw(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype b1 = this->RandomAccessFile::read(eof);
	inttype b2 = this->RandomAccessFile::read(eof);
	inttype b3 = this->RandomAccessFile::read(eof);
	inttype b4 = this->RandomAccessFile::read(eof);

	return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
}
#endif

inline longtype RandomAccessFile::readLong(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	return ((longtype) (readInt(eof)) << 32) + (readInt(eof) & 0xffffffffL);
}

inline float RandomAccessFile::readFloat(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	inttype v = readInt(eof);
	union {
		inttype i;
		float f;
	} model;
	model.i = (long) v;
	return (float) model.f;
}

inline double RandomAccessFile::readDouble(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	long v = readLong(eof);
	union {
		longtype l;
		double d;
	} model;
	model.l = v;
	return (double) model.d;
}

inline String* RandomAccessFile::readUTF(boolean* eof) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	// TODO: UTF
	inttype length = read(eof);
	nbyte bytes(length);
	read(&bytes, eof);
	return new String(&bytes);
}

/*
inline String* RandomAccessFile::readLine(boolean* eof) {
	reutrn null;
}
*/

inline void RandomAccessFile::writeBoolean(boolean v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write(v ? 1 : 0);
}

inline void RandomAccessFile::writeByte(inttype v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write(v);
}

inline void RandomAccessFile::writeShort(inttype v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write((v >> 8) & 0xff);
	write((v >> 0) & 0xff);
}

inline void RandomAccessFile::writeChar(inttype v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write((v >> 8) & 0xff);
	write((v >> 0) & 0xff);
}

inline void RandomAccessFile::writeInt(inttype v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write((v >> 24) & 0xff);
	write((v >> 16) & 0xff);
	write((v >>  8) & 0xff);
	write((v >>  0) & 0xff);
}

inline void RandomAccessFile::writeLong(longtype v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	write((int) (v >> 56) & 0xff);
	write((int) (v >> 48) & 0xff);
	write((int) (v >> 40) & 0xff);
	write((int) (v >> 32) & 0xff);
	write((int) (v >> 24) & 0xff);
	write((int) (v >> 16) & 0xff);
	write((int) (v >>  8) & 0xff);
	write((int) (v >>  0) & 0xff);
}

inline void RandomAccessFile::writeFloat(float v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	union {
		inttype i;
		float f;
	} model;
	model.f = (float) v;
	writeInt(model.i);
}

inline void RandomAccessFile::writeDouble(double v) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	union {
		longtype l;
		double d;
	} model;
	model.d = (double) v;
	writeLong(model.l);
}

inline void RandomAccessFile::writeUTF(const String* str) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	// TODO: UTF
	writeChars(str);
}

inline void RandomAccessFile::writeBytes(const String* str) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	writeChars(str);
}

inline void RandomAccessFile::writeChars(const String* str) {
	CXX_LANG_MEMORY_DEBUG_ASSERT(this);
	nbyte bytes((const bytearray) str->data(), str->length());
	write(bytes.length);
	write(&bytes);
}

} } // namespace

#endif /*CXX_IO_RANDOMACCESSFILE_H_*/
