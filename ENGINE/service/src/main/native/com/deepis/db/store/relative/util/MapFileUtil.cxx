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
#include <time.h>

#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/util/Versions.h"
#include "com/deepis/db/store/relative/util/MapFileUtil.h"
#include "com/deepis/db/store/relative/util/InvalidException.h"
#include "com/deepis/db/store/relative/util/PermissionException.h"

using namespace cxx::util;
using namespace com::deepis::db::store::relative::core;
using namespace com::deepis::db::store::relative::util;

const String MapFileUtil::FILE_INFIX = ".";
const String MapFileUtil::FILE_SUFFIX_IRT = ".irt";
const String MapFileUtil::FILE_SUFFIX_ERT = ".ert";
const String MapFileUtil::FILE_SUFFIX_LRT = ".lrt";
const String MapFileUtil::FILE_SUFFIX_VRT = ".vrt";
const String MapFileUtil::FILE_SUFFIX_TRT = ".trt";
const String MapFileUtil::FILE_SUFFIX_SRT = ".srt";
const String MapFileUtil::FILE_SUFFIX_XRT = ".xrt";

static boolean hasZeroSuffix(RandomAccessFile& rfile) {
	nbyte buf(102400);
	buf.zero();
	boolean eof = false;
	try {
		while (eof == false) {
			rfile.readFully(&buf, 0, buf.length, &eof);
			if (buf.isZero() == false) {
				return false;
			}
		}

	} catch (EOFException e) {
	}
	return buf.isZero();
}

boolean MapFileUtil::move(const String& source, const String& target) {
	boolean status = true;

	String sourceDir(".");
	String sourceName = source;

	int index = sourceName.lastIndexOf(File::separator);
	if (index != -1) {
		sourceDir = sourceName.substring(0, index);
		sourceName = sourceName.substring(index + 1, sourceName.length() - 1);
	}

	String targetDir(".");
	String targetName = target;

	index = targetName.lastIndexOf(File::separator);
	if (index != -1) {
		targetDir = targetName.substring(0, index);
		targetName = targetName.substring(index + 1, targetName.length() - 1);
	}

	ArrayList<String*>* list = File(sourceDir).list();
	if (list != null) {
		const String irtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_IRT;
		const String lrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_LRT;
		const String vrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_VRT;
		const String srtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_SRT;
		const String xrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_XRT;
		
		for (int i = 0; i < list->size(); i++) {
			String* item = list->get(i);

			if (item->endsWith(irtSuffix) == false &&
				item->endsWith(lrtSuffix) == false &&
				item->endsWith(vrtSuffix) == false &&
				item->endsWith(srtSuffix) == false &&
				item->endsWith(xrtSuffix) == false) {
				continue;
			}

			String from = sourceDir + File::separator + *item;
			String to = targetDir + File::separator + item->replace(sourceName, targetName);

			if (Files::isSymbolicLink(from) == true) {
				String fromTarget = Files::readSymbolicLink(from);
				String fromDir = "." + File::separator;
				index = fromTarget.lastIndexOf(File::separator);
				if (index != -1) {
					fromDir = fromTarget.substring(0, index + 1);
				}

				String toTarget = fromDir /* XXX: for now, fromDir == toDir */ + item->replace(sourceName, targetName);
				if (FileUtil::move(fromTarget, toTarget) == false) {
					DEEP_LOG(ERROR, OTHER, "Failure to move file: %s, %s\n", fromTarget.data(), toTarget.data());
					status = false;

					continue;
				}

				if (FileUtil::clobber(from, false /* followSymlinks */) == false) {
					DEEP_LOG(ERROR, OTHER, "Failure to remove symlink: %s\n", from.data());
					status = false;

					continue;
				}

				if (Files::createSymbolicLink(to, toTarget) == false) {
					DEEP_LOG(ERROR, OTHER, "Failure to create symlink: %s => %s\n", to.data(), toTarget.data());
					status = false;

					continue;
				}

				continue;
			}

			if (FileUtil::move(from, to) == false) {
				DEEP_LOG(ERROR, OTHER, "Failure to move file: %s, %s\n", from.data(), to.data());

				#if 0
				throw PermissionException("Failure to move file");
				#else
				status = false;
				#endif
			}
		}

		delete list;
	}

	return status;
}

boolean MapFileUtil::clobber(const String& fileName, const boolean values, uinttype* remainingFiles) {
	boolean status = true;

	String dname(".");
	String sourceName = fileName;

	int index = sourceName.lastIndexOf(File::separator);
	if (index != -1) {
		dname = sourceName.substring(0, index);
		sourceName = sourceName.substring(index + 1, sourceName.length() - 1);
	}
	
	ArrayList<String*>* list = File(dname).list();
	if (list != null) {
		const String irtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_IRT;
		const String lrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_LRT;
		const String vrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_VRT;
		const String srtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_SRT;
		const String xrtSuffix = MapFileUtil::FILE_INFIX + sourceName + MapFileUtil::FILE_SUFFIX_XRT;
		
		uinttype othersCount = 0;
		for (int i = 0; i < list->size(); i++) {
			String* item = list->get(i);
			
			if ((item->endsWith(irtSuffix) == true || 
				  (values == true &&
					(item->endsWith(lrtSuffix) == true ||
					 item->endsWith(vrtSuffix) == true ||
					 item->endsWith(srtSuffix) == true ||
					 item->endsWith(xrtSuffix) == true))) == false) {
				othersCount++;
				continue;
			}

			String file = dname + File::separator + *item;
			if (FileUtil::clobber((const char*)file, true /* followSymlinks */) == false) {
				DEEP_LOG(ERROR, OTHER, "Failure to delete file: %s\n", file.data());

				#if 0
				throw PermissionException("Failure to delete file");
				#else
				status = false;
				#endif
			}
		}

		if (remainingFiles != null) {
			(*remainingFiles) = othersCount;
		}
		delete list;
	}
	
	return status;
}

boolean MapFileUtil::deepFilesExist(const String& fileName) {
	String dname(".");

	int index = fileName.lastIndexOf(File::separator);
	if (index != -1) {
		dname = fileName.substring(0, index);
	}
	
	boolean found = false;
	ArrayList<String*>* list = File(dname).list();

	if (list != null) {
		for (int i = 0; i < list->size(); i++) {
			String* item = list->get(i);
			
			if ((item->endsWith(MapFileUtil::FILE_SUFFIX_IRT) == true || 
				item->endsWith(MapFileUtil::FILE_SUFFIX_LRT) == true ||
				item->endsWith(MapFileUtil::FILE_SUFFIX_VRT) == true ||
				item->endsWith(MapFileUtil::FILE_SUFFIX_SRT) == true ||
				item->endsWith(MapFileUtil::FILE_SUFFIX_XRT) == true)) {
				
				found = true;
				break;
			}

		}

		delete list;
	}
	
	return found;
}

boolean MapFileUtil::validate(File& file, FileType type, const longtype inSchema, const inttype size, longtype* protocol) {

	#ifdef DEEP_DEBUG
	switch(type) {
		case IRT:
		case LRT:
		case VRT:
			break;
		default:
			DEEP_LOG(ERROR, OTHER, "Invalid file type for header validation: %s (%d)\n", file.getPath(), type);
			throw InvalidException("Invalid file type for header validation");
	}
	#endif

	if (file.length() < Properties::DEFAULT_FILE_HEADER) {
		return false;
	}

	RandomAccessFile rfile(file, "r");
	rfile.setOnline(true); /* TODO: m_share.acquire(...); */

	if (rfile.length() < size) {
		DEEP_LOG(ERROR, OTHER, "Invalid file size for reading version information: %s\n", file.getPath());

		throw InvalidException("Invalid file size for reading version information");
	}

	rfile.seek(0);

	const inttype major = rfile.readInt();
	const inttype minor = rfile.readInt();
	const inttype revision = rfile.readInt();
	const inttype build = rfile.readInt();

	const longtype outProtocol = rfile.readLong();
	const longtype outSchema = rfile.readLong();

	// XXX: check to see if the file is suffixed by 0's; attempt recovery in this case
	if ((outProtocol == 0) && (outSchema == 0) && (hasZeroSuffix(rfile) == true)) {
		return false;
	}

	nbyte buffer(100);
	sprintf(buffer, "%d.%d.%d.%d : %lld.%lld", major, minor, revision, build, outProtocol, outSchema);

	const longtype inProtocol = Versions::getProtocolVersion();

	// TODO: correctly check full version - this is a workaround for the file version not being incremented when initially moving to 3.3
	#if 0
	if (outProtocol < Versions::PROTOCOL_MINIMUM) {
	#else
	if ((minor != 3) && (outProtocol < Versions::PROTOCOL_MINIMUM)) {
	#endif
		DEEP_LOG(ERROR, OTHER, "Invalid protocol when reading version information: %s / %lld, %s\n", (bytearray) buffer, inProtocol, file.getPath());

		if (type == IRT) {
			return false;
		}

		throw InvalidException("Invalid protocol when reading version information");
	}

	if (outSchema != inSchema) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema when reading version information: %s / %lld, %s\n", (bytearray) buffer, inSchema, file.getPath());

		if (type == IRT) {
			return false;
		}

		throw InvalidException("Invalid schema when reading version information");
	}

	DEEP_LOG(DEBUG, VERSN, "block: %s, proto: %s\n", file.getPath(), (bytearray) buffer);

	*protocol = outProtocol;
	return true;
}

void MapFileUtil::version(BufferedRandomAccessFile* file, const longtype schema, const inttype size) {

	if (file->length() != 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid length for writing version information: %s\n", file->getPath());

		throw InvalidException("Invalid length for writing version information");
	}

	if (file->getCursor() != 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid position for writing version information: %s\n", file->getPath());

		throw InvalidException("Invalid position for writing version information");
	}

	if (size < Versions::LENGTH) {
		DEEP_LOG(ERROR, OTHER, "Invalid header for writing version information: %s\n", file->getPath());

		throw InvalidException("Invalid header for writing version information");
	}

	file->setLength(size);
	file->seek(0);

	file->writeInt(Versions::getMajorVersion());
	file->writeInt(Versions::getMinorVersion());
	file->writeInt(Versions::getRevisionNumber());
	file->writeInt(Versions::getBuildNumber());

	file->writeLong(Versions::getProtocolVersion());
	file->writeLong(schema);

	file->flush();
	file->seek(size);
}

void MapFileUtil::format(char* buffer, const inttype index, const boolean date, const int size) {

	if (date == true /* dating file are for stream types (e.g. lrt/vrt/irt) */) {
		time_t tv;
		struct tm* tm;

		tv = time(null);
		tm = localtime(&tv);
		if (tm == null) {
			DEEP_LOG(ERROR, OTHER, "Invalid file time access\n");

			throw InvalidException("Invalid file time access");
		}

		// XXX: "YYYY_mm_dd_HH_MM_SS." where data is size - 5 characters
		if (strftime(buffer, size - 5, "%Y_%m_%d_%H_%M_%S", tm) == 0) {
			DEEP_LOG(ERROR, OTHER, "Invalid time format\n");

			throw InvalidException("Invalid time format");
		}

	} else {
		strcpy(buffer, "0000_00_00_00_00_00");
	}

	// XXX: "YYYY_mm_dd_HH_MM_SS.index." where index is 5 characters
	sprintf(buffer + strlen(buffer), ".%05d.", index);
}

time_t MapFileUtil::fileCreationTime(const String& fileName) {

	struct tm tm;
	memset(&tm, 0, sizeof(struct tm));

	strptime(fileName.c_str(), "%Y_%m_%d_%H_%M_%S", &tm);

	return mktime(&tm);
}
