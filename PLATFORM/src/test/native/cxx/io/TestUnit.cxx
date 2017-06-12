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
#include "cxx/io/File.h"
#include "cxx/io/RandomAccessFile.h"

#include "cxx/util/Logger.h"
#include "cxx/util/Collections.h"

using namespace std;
using namespace cxx::io;

static int testSyncFilePointer() {
	struct Test {
		File file;
		Test() : file("file.dat") {}
		~Test() { file.clobber(); }
	} test;
	RandomAccessFile writer(&test.file, "rw");
	
	if(writer.getFilePointer() != 0) {
		DEEP_LOG(ERROR, OTHER, " -- FILE: Initial file position: %lld\n", writer.getFilePointer());
		return 1;
	}
	
	FILE* fp = writer.getHandle();
	int i;
	for(i=1; i<=42; ++i) {
		fputc(i,fp);
	}
	--i;
	
	if(writer.getFilePointer() != 0) {
		DEEP_LOG(ERROR, OTHER, " -- FILE: Pre-sync file position: %lld\n", writer.getFilePointer());
		return 1;
	}
	
	writer.syncFilePointer();
	
	if(writer.getFilePointer() != i) {
		DEEP_LOG(ERROR, OTHER, " -- FILE: Post-sync file position: %lld\n", writer.getFilePointer());
		return 1;
	}
	
	return 0;
}


int main(int argc, char** argv) {
	int success = 0;
	
	RandomAccessFile writer("file.dat", "rw");
	writer.setLength(0);
	writer.seek(0);
	writer.writeInt(123);
	writer.writeLong(456);
	writer.close();

	RandomAccessFile reader("file.dat", "r");
	reader.seek(0);
	int x = reader.readInt();
	DEEP_LOG(INFO, OTHER, "INT: %d\n", x);
	long long y = reader.readLong();
	DEEP_LOG(INFO, OTHER, "LONG: %lld\n", y);
	reader.close();

	File file("file.dat");
	file.renameTo("rename.dat");
	file.clobber();

	File dir(".");
	ArrayList<String*>* list = dir.list();
	if (list != null) {
		DEEP_LOG(INFO, OTHER, "REGULAR:\n");
		for (int i = 0; i < list->size(); i++) {
			DEEP_LOG(INFO, OTHER, " -- FILE: %s\n", list->get(i)->data());
		}

		Collections::reverse(list);

		DEEP_LOG(INFO, OTHER, "REVERSE:\n");
		for (int i = 0; i < list->size(); i++) {
			DEEP_LOG(INFO, OTHER, " -- FILE: %s\n", list->get(i)->data());
		}

		Collections::reverse(list);

		DEEP_LOG(INFO, OTHER, "REGULAR-AGAIN:\n");
		for (int i = 0; i < list->size(); i++) {
			DEEP_LOG(INFO, OTHER, " -- FILE: %s\n", list->get(i)->data());
		}

		delete list;
	}
	
	success |= testSyncFilePointer();

	return success;
}
