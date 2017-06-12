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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BUFFEREDRANDOMACCESSFILE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BUFFEREDRANDOMACCESSFILE_H_ 

#include <zlib.h>

#include "cxx/util/Logger.h"
#include "cxx/io/IOException.h"
#include "cxx/io/EOFException.h"
#include "cxx/io/RandomAccessFile.h"
#include "com/deepis/db/store/relative/util/InvalidException.h"

using namespace std;
using namespace cxx::io;
using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class BufferedRandomAccessFile : public RandomAccessFile {

	public:
		enum CompressMode {
			COMPRESS_NONE,
			COMPRESS_WRITE,
			COMPRESS_READ
		};

	private:

		enum FinalizeMode {
			FINALIZE_NONE,
			FINALIZE_BLOCK,
			FINALIZE_DONE
		};

		static const ubytetype SIZE_RESERVE = (2 * sizeof(uinttype));
		static const uinttype MAX_UNCOMPRESSED_SIZE = /* 100M */ 104857600;  

		nbyte m_buffer;
		nbyte* m_zipBuffer;
		longtype m_length;
		longtype m_position;
		longtype m_compressTotal;
		floattype m_compressionRatio;
		inttype m_blockCompressionCursor;
		inttype m_cursor;
		inttype m_offset;
		inttype m_memory;
		boolean m_notify:1;
		boolean m_flushed:1;
		boolean m_finalBlockInSeries:1;

		CompressMode m_compressMode;
		inttype m_compressStart;
		longtype m_blockLength;

		#ifdef DEEP_DEBUG
		uinttype m_lastUncompressedBlockLength;
		#endif

		z_stream* m_inZstream;

		boolean m_refill;

	private:

		uinttype compressToBuffer(const nbyte* bytes, int offset, int length, FinalizeMode finalizeMode);
		uinttype compressToBuffer(bytetype b);
		void blockCompression(void);
		void initializeCompression(void);
		void finalizeCompression(FinalizeMode finalizeMode);

		virtual void attach(void);
		virtual void detach(void);

	public:
		void fill(boolean* eof = null, boolean validate = false);
		static const inttype BUFFER_SIZE;

	public:
		// XXX: "only legal" when using this class as a buffer
		BufferedRandomAccessFile(inttype bufferSize);

		FORCE_INLINE void setWriter(BufferedRandomAccessFile* writer) {
			m_writer = writer;
		}

		FORCE_INLINE BufferedRandomAccessFile* getWriter() {
			return (BufferedRandomAccessFile*) m_writer;
		}

		/* XXX: available bytes to read from buffer */
		FORCE_INLINE inttype available() const {
			return m_offset;
		}

		FORCE_INLINE void setCursor(inttype cursor) {
			m_blockCompressionCursor = 0;
			m_cursor = cursor;
		}

		FORCE_INLINE inttype getCursor() const {
			return m_blockCompressionCursor + m_cursor;
		}

		FORCE_INLINE void setPosition(longtype position) {
			m_position = position;
		}

		FORCE_INLINE longtype getPosition() const {
			return m_position;
		}

		FORCE_INLINE void setCompressTotal(longtype total) {
			m_compressTotal = total;
		}

		FORCE_INLINE longtype getCompressTotal() const {
			return m_compressTotal;
		}

		FORCE_INLINE void resetCompressionRatio() {
			m_compressionRatio = 0;
		}

		FORCE_INLINE doubletype getCompressionRatio() const {
			return m_compressionRatio;
		}

		FORCE_INLINE void setNofityFlushed(boolean notify) {
			if (notify == false) {
				m_flushed = false;
			}

			m_notify = notify;
		}

		FORCE_INLINE boolean getNofityFlushed() const {
			return m_notify;
		}

		FORCE_INLINE boolean flushable() const {
			return m_cursor != 0;
		}

		FORCE_INLINE boolean flushed() const {
			return m_flushed;
		}

		FORCE_INLINE CompressMode getCompress() const {
			return m_compressMode;
		}

		FORCE_INLINE longtype getAndResetBlockLength() {
			longtype length = m_blockLength;
			m_blockLength = -1;
			return length;
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE uinttype getLastUncompressedBlockLength() const {
			return m_lastUncompressedBlockLength;
		}
		#endif

		FORCE_INLINE void clear() {
			m_blockCompressionCursor = 0;
			m_cursor = 0;
			m_position = 0;
		}

		FORCE_INLINE void setCompress(CompressMode mode) {		

			// XXX: only allow NONE->READ, READ->NONE, NONE->WRITE, WRITE->NONE
			#ifdef DEEP_DEBUG
			if ((m_compressMode == mode) || ((m_compressMode != COMPRESS_NONE) && (mode != COMPRESS_NONE))) {
				DEEP_LOG(ERROR, OTHER, "Invalid mode: compression request %d, %d\n", mode, m_compressMode);

				throw InvalidException("Invalid mode: compression request");
			}
			#endif
	
			if (m_compressMode == COMPRESS_READ || mode == COMPRESS_READ) {
				m_refill = true;
				m_blockLength = -1;
				m_finalBlockInSeries = false;
				m_blockCompressionCursor = 0;
			}

			if (m_compressMode == COMPRESS_WRITE) {
				inttype written = compressToBuffer(null, 0, 0, FINALIZE_DONE);
				m_cursor += written;
				m_position += written;
			}

			m_compressMode = mode;
		}

	public:
		BufferedRandomAccessFile(const char* path, const char* mode, inttype bufferSize = BUFFER_SIZE);
		BufferedRandomAccessFile(const File* file, const char* mode, inttype bufferSize = BUFFER_SIZE);

		virtual ~BufferedRandomAccessFile() {
			//XXX: could be allocated in case of exceptions thrown from fill method
			delete m_zipBuffer; 
		}

		FORCE_INLINE void flush();

		FORCE_INLINE virtual int read(boolean* eof = null);
		FORCE_INLINE virtual int read(nbyte* bytes, boolean* eof = null);
		FORCE_INLINE virtual int read(nbyte* bytes, int offset, int length, boolean* eof = null);

		FORCE_INLINE void readFullyRaw(nbyte* bytes, int offset, int length, boolean* eof = null);

		FORCE_INLINE virtual void write(int b);
		FORCE_INLINE virtual void write(const nbyte* bytes);
		FORCE_INLINE virtual void write(const nbyte* bytes, int offset, int length);

		FORCE_INLINE void writeRaw(const nbyte* bytes, int offset, int length);

		FORCE_INLINE virtual int skipBytes(int n, boolean* eof = null);

		FORCE_INLINE virtual longtype getFilePointer();
		FORCE_INLINE virtual void seek(longtype pos, boolean validate = false);

		virtual void close();
};

FORCE_INLINE void BufferedRandomAccessFile::flush() {
	#ifdef DEEP_DEBUG
	if (getHandle() == null) {
		throw IOException("Buffer capacity has been reached");
	}

	if (m_compressMode != COMPRESS_NONE) {
		throw new InvalidException("Invalid flush request");
	}
	#endif

	if (m_cursor != 0) {

		RandomAccessFile::write(&m_buffer, 0, m_cursor);

		if (m_notify == true) {
			m_flushed = true;
		}

		m_cursor = 0;
	}
}

FORCE_INLINE int BufferedRandomAccessFile::read(boolean* eof) {

	#ifdef DEEP_DEBUG
	if (m_length == -1 /* note unit tests and ensured flush */) {
		throw EOFException("Read/Write not supported");
	}

	if (m_refill == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid read: buffer has not been refilled\n");

		throw InvalidException("Invalid read: buffer has not been refilled");
	}
	#endif

	RETRY:
	if ((m_cursor + 1) > available()) {
		#ifdef DEEP_DEBUG
		if ((m_compressMode != COMPRESS_NONE) && (m_finalBlockInSeries == true)) {
			DEEP_LOG(ERROR, OTHER, "Invalid compression: read length exceeds compressed block (1)\n");

			throw InvalidException("Invalid compression: read length exceeds compressed block (1)");
		}
		#endif

		boolean fillEof = false;
		fill(&fillEof);
		if (available() > 0) {
			goto RETRY;
		} else {
			if (eof != null) {
				*eof |= fillEof;
			}
			return -1;
		}
	}

	bytetype b = m_buffer[m_cursor];
	m_cursor++;

	int diff = available() - m_cursor;
	if ((diff == 0) && ((m_compressMode == COMPRESS_NONE) || (m_finalBlockInSeries == false))) {
		fill();
	}

	return ((int) b) & 0xff;
}

FORCE_INLINE int BufferedRandomAccessFile::read(nbyte* bytes, boolean* eof) {

	return read(bytes, 0, bytes->length, eof);
}

FORCE_INLINE int BufferedRandomAccessFile::read(nbyte* bytes, int offset, int length, boolean* eof) {

	#ifdef DEEP_DEBUG
	if (m_length == -1 /* note unit tests and ensured flush */) {
		throw EOFException("Read/Write not supported");
	}

	if (m_refill == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid read: buffer has not been refilled\n");

		throw InvalidException("Invalid read: buffer has not been refilled");
	}
	#endif

	inttype bytesRead = 0;
	RETRY:
	if ((m_cursor + length) > available()) {

		#ifdef DEEP_DEBUG
		if ((m_compressMode != COMPRESS_NONE) && (m_finalBlockInSeries == true)) {
			DEEP_LOG(ERROR, OTHER, "Invalid compression: read length exceeds compressed block (2)\n");

			throw InvalidException("Invalid compression: read length exceeds compressed block (2)");
		}
		#endif

		int diff = available() - m_cursor;
		System::arraycopy(&m_buffer, m_cursor, bytes, offset, diff);

		boolean fillEof = false;
		fill(&fillEof);

		offset += diff;
		length -= diff;
		bytesRead += diff;
		if (available() > 0) {
			goto RETRY;
		} else if (eof != null) {
			*eof |= fillEof;
		}

	} else {
		System::arraycopy(&m_buffer, m_cursor, bytes, offset, length);
		m_cursor += length;
		bytesRead += length;
	}

	int diff = available() - m_cursor;

	if ((diff == 0) && ((m_compressMode == COMPRESS_NONE) || (m_finalBlockInSeries == false))) {
		fill();
	}

	return bytesRead;
}

FORCE_INLINE void BufferedRandomAccessFile::readFullyRaw(nbyte* bytes, int offset, int length, boolean* eof) {

	#ifdef DEEP_DEBUG
	if (m_compressMode != COMPRESS_NONE) {
		DEEP_LOG(ERROR, OTHER, "Invalid compression: read fully raw not supported\n");

		throw InvalidException("Invalid compression: read fully raw not supportes");
	}
	#endif

	RandomAccessFile::readFully(bytes, offset, length, eof);
}

FORCE_INLINE void BufferedRandomAccessFile::write(int b) {

	uinttype length = 1;
	if (m_compressMode == COMPRESS_WRITE) {
		length = compressToBuffer((bytetype)b);

	} else {
		if (m_cursor >= m_buffer.length) {
			flush();
		}

		m_buffer[m_cursor] = (bytetype) b;
		m_blockLength++;
	}

	m_cursor += length;
	m_position += length;
}

FORCE_INLINE void BufferedRandomAccessFile::write(const nbyte* bytes) {
	 write(bytes, 0, bytes->length);
}

FORCE_INLINE void BufferedRandomAccessFile::write(const nbyte* bytes, int offset, int length) {

	if (m_compressMode == COMPRESS_WRITE) {

		length = compressToBuffer(bytes, offset, length, FINALIZE_NONE);

	} else {

		if ((m_cursor + length) >= m_buffer.length) {

			flush();

			// XXX: handle changes greater than internal buffer
			if (length >= m_buffer.length) {
				RandomAccessFile::write(bytes, offset, length);
				m_position += length;
				return;
			}
		}

		System::arraycopy(bytes, offset, &m_buffer, m_cursor, length);
		m_blockLength += length;
	}

	m_cursor += length;
	m_position += length;
}

FORCE_INLINE void BufferedRandomAccessFile::writeRaw(const nbyte* bytes, int offset, int length) {

	#ifdef DEEP_DEBUG
	if (m_compressMode != COMPRESS_NONE) {
		DEEP_LOG(ERROR, OTHER, "Invalid compression: write raw not supported\n");

		throw InvalidException("Invalid compression: write raw not supportes");
	}
	#endif

	flush();

	RandomAccessFile::write(bytes, offset, length);
	m_position += length;
}

FORCE_INLINE int BufferedRandomAccessFile::skipBytes(int n, boolean* eof) {

	#ifdef DEEP_DEBUG
	if (m_length == -1 /* note unit tests and ensured flush */) {
		throw EOFException("Read/Write not supported");
	}

	if (m_refill == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid read: buffer has not been refilled\n");

		throw InvalidException("Invalid read: buffer has not been refilled");
	}
	#endif

	inttype bytesSkipped = 0;
	RETRY:
	if ((m_cursor + n) > available()) {

		#ifdef DEEP_DEBUG
		if ((m_compressMode != COMPRESS_NONE) && (m_finalBlockInSeries == true)) {
			DEEP_LOG(ERROR, OTHER, "Invalid compression: skipping past compressed block\n");

			throw InvalidException("Invalid compression: skipping past compressed block");
		}
		#endif

		int diff = available() - m_cursor;
		boolean fillEof = false;
		fill(&fillEof);

		n -= diff;
		bytesSkipped += diff;
		if (available() > 0) {
			goto RETRY;
		} else if (eof != null) {
			*eof |= fillEof;
		}

	} else {
		m_cursor += n;
		bytesSkipped += n;
	}

	return bytesSkipped;
}

FORCE_INLINE longtype BufferedRandomAccessFile::getFilePointer() {

	#ifdef DEEP_DEBUG
	if (m_compressMode == COMPRESS_READ) {
		DEEP_LOG(ERROR, OTHER, "Invalid file pointer: file is in COMPRESS_READ mode\n");

		throw InvalidException("Invalid file pointer: file is in COMPRESS_READ mode");
	}
	#endif

	if ((getHandle() == null) || (m_length == -1 /* note unit tests and ensured flush */)) {
		return m_position;

	} else {
		return (m_position - m_offset) + m_cursor;
	}
}

FORCE_INLINE void BufferedRandomAccessFile::seek(longtype pos, boolean validate) {

	#ifdef DEEP_DEBUG
	if (m_compressMode == COMPRESS_WRITE) {
		DEEP_LOG(ERROR, OTHER, "Invalid compression: cannot seek in COMPRESS_WRITE mode\n");

		throw InvalidException("Invalid compression: cannot seek in COMPRESS_WRITE mode");
	}
	#endif

	if (getHandle() != null) {
		if (m_length != -1 /* note unit tests and ensured flush */) {
			m_length = (m_writer != null) ? m_writer->length() : length();

			#ifdef DEEP_DEBUG
			if (pos > m_length) {
				DEEP_LOG(ERROR, OTHER, "Invalid seek: position greater than length\n");

				throw InvalidException("Invalid seek: position greater than length");
			}
			#endif

			if ((m_refill == true) || (m_compressMode == COMPRESS_READ) || (BufferedRandomAccessFile::getFilePointer() != pos)) {
				// XXX: don't seek if new position is somewhere within buffer
				longtype currentBufferStart = (m_position - m_offset);
				longtype currentBufferEnd = currentBufferStart + available();

				if ((m_refill == false) && (pos >= currentBufferStart) && (pos < currentBufferEnd)) {
					m_cursor = (pos - currentBufferStart); 
				} else {
					RandomAccessFile::seek(pos, m_length);
					m_position = pos;

					fill(null /* eof */, validate);
				}
			}

		} else {

			if (BufferedRandomAccessFile::getFilePointer() != pos) {
				flush();

				RandomAccessFile::seek(pos);
				m_position = pos;
			}
		}

	} else {
		m_cursor = 0;
		m_position = pos;
	}
}

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BUFFEREDRANDOMACCESSFILE_H_*/
