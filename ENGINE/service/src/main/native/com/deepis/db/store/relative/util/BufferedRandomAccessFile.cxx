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
#include "cxx/lang/System.h"

#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"

using namespace com::deepis::db::store::relative::util;

const inttype BufferedRandomAccessFile::BUFFER_SIZE = 65536;

BufferedRandomAccessFile::BufferedRandomAccessFile(inttype bufferSize):
	RandomAccessFile(),
	m_buffer(0),
	m_zipBuffer(null),
	m_length(-1),
	m_position(0),
	m_compressTotal(0),
	m_compressionRatio(0),
	m_blockCompressionCursor(0),
	m_cursor(0),
	m_offset(0),
	m_memory(bufferSize),
	m_notify(false),
	m_flushed(false),
	m_finalBlockInSeries(false),
	m_compressMode(COMPRESS_NONE),
	m_compressStart(0),
	m_blockLength(-1),
	#ifdef DEEP_DEBUG
	m_lastUncompressedBlockLength(0),
	#endif
	m_inZstream(null),
	m_refill(false) {
}

BufferedRandomAccessFile::BufferedRandomAccessFile(const char* path, const char* mode, inttype bufferSize):
	RandomAccessFile(path, mode, false /* online */),
	m_buffer(0),
	m_zipBuffer(null),
	m_length(-1),
	m_position(0),
	m_compressTotal(0),
	m_compressionRatio(0),
	m_blockCompressionCursor(0),
	m_cursor(0),
	m_offset(0),
	m_memory(bufferSize),
	m_notify(false),
	m_flushed(false),
	m_finalBlockInSeries(false),
	m_compressMode(COMPRESS_NONE),
	m_compressStart(0),
	m_blockLength(-1),
	#ifdef DEEP_DEBUG
	m_lastUncompressedBlockLength(0),
	#endif
	m_inZstream(null),
	m_refill(false) {
}

BufferedRandomAccessFile::BufferedRandomAccessFile(const File* file, const char* mode, inttype bufferSize):
	RandomAccessFile(file, mode, false /* online */),
	m_buffer(0),
	m_zipBuffer(null),
	m_position(0),
	m_compressTotal(0),
	m_compressionRatio(0),
	m_blockCompressionCursor(0),
	m_cursor(0),
	m_offset(0),
	m_memory(bufferSize),
	m_notify(false),
	m_flushed(false),
	m_finalBlockInSeries(false),
	m_compressMode(COMPRESS_NONE),
	m_compressStart(0),
	m_blockLength(-1),
	#ifdef DEEP_DEBUG
	m_lastUncompressedBlockLength(0),
	#endif
	m_inZstream(null),
	m_refill(false) {
}

void BufferedRandomAccessFile::blockCompression(void) {
	
	inttype written = compressToBuffer(null, 0, 0, FINALIZE_BLOCK);
	m_cursor += written;
	m_position += written;
	m_compressMode = COMPRESS_NONE;

	flush();
	m_compressMode = COMPRESS_WRITE;
}

uinttype BufferedRandomAccessFile::compressToBuffer(const nbyte* bytes, int offset, int length, FinalizeMode finalizeMode) {

	#ifdef DEEP_DEBUG
	if ((finalizeMode == FINALIZE_NONE) && (bytes == null)) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress to buffer invalid parameters %s\n", getPath());

		throw InvalidException("Buffered random access file error: compress to buffer invalid parameters");
	}
	#endif

	// XXX: force a new compressed block if we are going to exceed the max uncompressed size	
	if ((finalizeMode == FINALIZE_NONE) && (m_inZstream != null) && ((m_inZstream->total_in + length) >= MAX_UNCOMPRESSED_SIZE)) {
		blockCompression();
	}

	inttype code;
	boolean firstCycle = false;

	if (m_inZstream == null) {
		firstCycle = true;
		initializeCompression();
	}

	uinttype total_in_this_cycle = 0;
	uinttype total_in_before_this_cycle = m_inZstream->total_in;

	uinttype total_out_this_cycle = firstCycle ? SIZE_RESERVE : 0;
	uinttype total_out_before_this_cycle = m_inZstream->total_out;

	do {
		if ((firstCycle == false && total_out_this_cycle != 0) || (firstCycle == true && total_out_this_cycle != SIZE_RESERVE)) {
			m_buffer.extend(m_buffer.length * 2);
		}

		if (bytes == null) {
			m_inZstream->next_in   = null;
			m_inZstream->avail_in  = 0;
		} else {
			m_inZstream->next_in   = (Bytef*) (((bytearray)*bytes) + offset + total_in_this_cycle);
			m_inZstream->avail_in  = length - offset - total_in_this_cycle;
		}
		m_inZstream->next_out  = (Bytef*)(((bytearray)(m_buffer)) + m_cursor + total_out_this_cycle);
		m_inZstream->avail_out = m_buffer.length - m_cursor - total_out_this_cycle;

		bytetype flushFlag = (finalizeMode != FINALIZE_NONE) ? Z_FINISH : Z_NO_FLUSH;
		code = deflate(m_inZstream, flushFlag);
		if (code != Z_OK && code != Z_STREAM_END) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress with offset deflate failure code %d, %s\n", code, getPath());

			throw InvalidException("Buffered random access file error: compress with offset deflate failure");
		}

		total_in_this_cycle = m_inZstream->total_in - total_in_before_this_cycle;
		total_out_this_cycle = m_inZstream->total_out - total_out_before_this_cycle;

		if (firstCycle == true) {
			total_out_this_cycle += SIZE_RESERVE;
		}

	} while (code == Z_OK && m_inZstream->avail_out == 0);

	if (finalizeMode != FINALIZE_NONE) {
		finalizeCompression(finalizeMode);
	}

	return total_out_this_cycle;
}

uinttype BufferedRandomAccessFile::compressToBuffer(bytetype b) {
	inttype code;
	boolean firstCycle = false;

	// XXX: force a new compressed block if we are going to exceed the max uncompressed size	
	if ((m_inZstream != null) && ((m_inZstream->total_in + 1) >= MAX_UNCOMPRESSED_SIZE)) {
		blockCompression();
	}

	if (m_inZstream == null) {
		firstCycle = true;
		initializeCompression();
	}

	uinttype total_in_this_cycle = 0;
	uinttype total_in_before_this_cycle = m_inZstream->total_in;

	uinttype total_out_this_cycle = firstCycle ? SIZE_RESERVE : 0;
	uinttype total_out_before_this_cycle = m_inZstream->total_out;

	do {
		if ((firstCycle == false && total_out_this_cycle != 0) || (firstCycle == true && total_out_this_cycle != SIZE_RESERVE)) {
			m_buffer.extend(m_buffer.length * 2);
		}

		m_inZstream->next_in   = (Bytef*)&b;
		m_inZstream->avail_out = m_buffer.length - m_cursor - total_out_this_cycle;
		m_inZstream->next_out  = (Bytef*)(((bytearray)(m_buffer)) + m_cursor + total_out_this_cycle);
		m_inZstream->avail_in  = 1 - total_in_this_cycle;

		code = deflate(m_inZstream, Z_NO_FLUSH);
		if (code != Z_OK) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress with byte deflate failure code %d, %s\n", code, getPath());

			throw InvalidException("Buffered random access file error: compress with byte deflate failure");
		}

		total_in_this_cycle = m_inZstream->total_in - total_in_before_this_cycle;
		total_out_this_cycle = m_inZstream->total_out - total_out_before_this_cycle;

		if (firstCycle == true) {
			total_out_this_cycle += SIZE_RESERVE;
		}

	} while (code == Z_OK && m_inZstream->avail_out == 0);

	return total_out_this_cycle;
}

void BufferedRandomAccessFile::initializeCompression(void) {

	#ifdef DEEP_DEBUG
	if (m_compressStart != 0) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress start is not initial state %s\n", getPath());

		throw InvalidException("Buffered random access file error: compress start is not initial state");
	}
	#endif

	m_compressStart = m_cursor;

	m_inZstream = (z_stream*) malloc(sizeof(z_stream));
	m_inZstream->zfree = Z_NULL;
	m_inZstream->zalloc = Z_NULL;
	m_inZstream->opaque = Z_NULL;

	inttype code = deflateInit(m_inZstream, Z_BEST_SPEED);
	if (code != Z_OK) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress deflate init failure code %d, %s\n", code, getPath());

		throw InvalidException("Buffered random access file error: compress deflate init failure");
	}
}

void BufferedRandomAccessFile::finalizeCompression(FinalizeMode finalizeMode) {

	inttype code = deflateEnd(m_inZstream);
	if (code != Z_OK) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress deflate end failure code %d, %s\n", code, getPath());

		throw InvalidException("Buffered random access file error: compress deflate end failure");
	}

	// XXX: prefix compressed data with the compressed and uncompressed sizes
	uinttype total_in = m_inZstream->total_in;
	uinttype total_out = m_inZstream->total_out;

	m_compressionRatio = ((floattype) total_in) / total_out;

	// XXX: using top bit in uncompressed size (total_in) to indicate whether this is the final (set) or intermediate (unset) block in this series
	#ifdef DEEP_DEBUG
	if ((1 << 31) & total_in) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: illegal total_in value during finalize %d, %s\n", total_in, getPath());

		throw InvalidException("Buffered random access file error: illegal total_in value during finalize");
	}
	#endif 

	if (finalizeMode == FINALIZE_DONE) {
		total_in = (1 << 31) | total_in;
	}

	memcpy((bytearray)(m_buffer) + m_compressStart, &total_in, SIZE_RESERVE / 2);
	memcpy((bytearray)(m_buffer) + m_compressStart + (SIZE_RESERVE / 2), &total_out, SIZE_RESERVE / 2);

	m_compressStart = 0;
	m_compressTotal += total_out;
	m_blockLength = SIZE_RESERVE + total_out;

	free(m_inZstream);
	m_inZstream = null;
}

void BufferedRandomAccessFile::fill(boolean* eof, boolean validate) {
	/* XXX: ensure eof is non-null */
	if (eof == null) {
		eof = (boolean*)alloca(sizeof(boolean));
		*eof = false;
	}

	#ifdef DEEP_DEBUG
	if (m_compressMode == COMPRESS_WRITE) {
		DEEP_LOG(ERROR, OTHER, "Buffered random access file error: fill for compress write %s\n", getPath());

		throw InvalidException("Buffered random access file error: fill for compress write");
	}
	#endif

	if (m_compressMode == COMPRESS_READ) {
		nbyte sizeBuffer(SIZE_RESERVE);
		RandomAccessFile::readFullyRaw(&sizeBuffer, 0, SIZE_RESERVE, eof);
		if (*eof == true) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: unexpected EOF on compressed read %lld, %u, %s\n", m_position, SIZE_RESERVE, getPath());

			throw InvalidException("Buffered random access file error: unexpected EOF on compressed read");
		}

		m_position += SIZE_RESERVE;

		uinttype uncompressedSize = 0;
		uinttype compressedSize = 0;

		memcpy(&uncompressedSize, (bytearray)sizeBuffer, SIZE_RESERVE / 2);
		memcpy(&compressedSize, (bytearray)sizeBuffer + (SIZE_RESERVE / 2), SIZE_RESERVE / 2);

		// XXX: maintain a virtual cursor throughout an entire series of compressed blocks to remain transparent for clients
		if ((m_refill == false) /* do not add stale cursor count on new reads */  && (m_finalBlockInSeries == false)) {
			m_blockCompressionCursor += m_cursor;
		} else {
			m_blockCompressionCursor = 0;
		}

		if ((1 << 31) & uncompressedSize) {
			m_finalBlockInSeries = true;
			uncompressedSize = (1 << 31) ^ uncompressedSize;

		} else {
			m_finalBlockInSeries = false;
		}

		#ifdef DEEP_DEBUG
		m_lastUncompressedBlockLength = uncompressedSize;
		#endif

		m_blockLength = SIZE_RESERVE + compressedSize;

		#ifdef DEEP_DEBUG
		validate = true;
		#endif
		if ((validate == true) && ((uncompressedSize < 0) || (uncompressedSize > MAX_UNCOMPRESSED_SIZE))) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: uncompress size %d, %s\n", uncompressedSize, getPath());

			throw InvalidException("Buffered random access file error: uncompress size");
		}

		if ((validate == true) && ((compressedSize < 0) || (compressedSize > MAX_UNCOMPRESSED_SIZE))) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: compress size: %d, %s\n", compressedSize, getPath());

			throw InvalidException("Buffered random access file error: compress size");
		}

		m_zipBuffer = new nbyte(compressedSize);

		if (uncompressedSize > (uinttype) m_buffer.length) {
			m_buffer.extend(uncompressedSize);
		}

		RandomAccessFile::readFullyRaw(m_zipBuffer, 0, compressedSize, eof);
		if (*eof == true) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: unexpected EOF on compressed read %lld, %u, %s\n", m_position, compressedSize, getPath());

			throw InvalidException("Buffered random access file error: unexpected EOF on compressed read");
		}

		// XXX: position should be increased by the amount read from the actual file, ie compressed size
		m_position += compressedSize;

		z_stream outZstream;

		outZstream.zfree = Z_NULL;
		outZstream.zalloc = Z_NULL;
		outZstream.opaque = Z_NULL;

		inttype code = inflateInit(&outZstream);
		if (code != Z_OK) {
			DEEP_LOG(ERROR, OTHER, "Buffered random access file error: fill compressed inflate init failure code %d, %s\n", code, getPath());

			throw InvalidException("Buffered random access file error: fill compressed inflate init failure");
		}

		do {
			outZstream.avail_out = m_buffer.length - outZstream.total_out;
			outZstream.next_out = (Bytef*) (((bytearray)m_buffer) + outZstream.total_out);
			outZstream.next_in = (Bytef*) (((bytearray)*m_zipBuffer) + outZstream.total_in);
			outZstream.avail_in = m_zipBuffer->length - outZstream.total_in;

			code = inflate(&outZstream, Z_FINISH);
			if (code != Z_STREAM_END) {
				DEEP_LOG(ERROR, OTHER, "Buffered random access file error: fill compressed inflate failure code %d, %s\n", code, getPath());

				throw InvalidException("Buffered random access file error: fill compressed inflate failure");
			}

		} while (code == Z_OK && outZstream.avail_out == 0);

		inflateEnd(&outZstream);

		// XXX: offset should be set to available data, ie the amount we have decompressed
		m_offset = outZstream.total_out;

		delete m_zipBuffer;
		m_zipBuffer = null;

	} else {
		inttype total = m_buffer.length;

		m_offset = 0;
		while ((*eof == false) && (total > 0)) {
			inttype size = RandomAccessFile::read(&m_buffer, m_offset, total, eof);
			if (size < 0) {
				break;
			}

			m_offset += size;
			total -= size;
		}

		m_position += m_offset;
	}

	m_cursor = 0;
	m_refill = false;
}

void BufferedRandomAccessFile::attach(void) {
	RandomAccessFile::attach();

	m_buffer.realloc(m_memory);
	#ifdef DEEP_DEBUG
	m_buffer.zero();
	#endif

	clear();

	if (strcmp(m_mode, "r") == 0) {
		/* XXX: specifying buffer seems get parallel fread out of sync with fwrite
		if (setvbuf(getHandle(), m_buffer, _IOFBF, m_buffer.length) != 0) {
			throw IOException("failure: setvbuf for read");
		}
		*/

		// XXX: attribute is used to indicate read/write mode (i.e. set in readonly)
		m_length = length();

		RandomAccessFile::seek(0);

		fill();

	} else {
		/* XXX: specifying buffer seems get parallel fread out of sync with fwrite
		if (setvbuf(getHandle(), null, _IONBF, 0) != 0) {
			throw IOException("failure: setvbuf for write");
		}
		*/

		m_position = length();

		RandomAccessFile::seek(m_position);
	}
}

void BufferedRandomAccessFile::detach(void) {
	RandomAccessFile::detach();

	m_buffer.realloc(0);
}

void BufferedRandomAccessFile::close() {
	if (getHandle() != null) {
		if (m_length == -1) {
			flush();
		}

		RandomAccessFile::close();

		m_buffer.realloc(0);
	}
}
