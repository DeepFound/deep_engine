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
#include "cxx/lang/Object.h"
#include "cxx/lang/nbyte.h"
#include "cxx/lang/String.h"

#include "cxx/util/Logger.h"

using namespace cxx::lang;
using namespace cxx::util;

void testByte1(char* bytes) {
	DEEP_LOG(INFO, OTHER, "DATA-1: %s\n", bytes);
	//bytes[0] = 'a';
}

void testByte2(void* bytes) {
	DEEP_LOG(INFO, OTHER, "DATA-2: %s\n", (char*) bytes);
	//bytes[1] = 'b';
}

void testByte3(nbyte* bytes) {
	DEEP_LOG(INFO, OTHER, "DATA-3: %s\n", (char*) *bytes);
	//*bytes[2] = 'c';

	const char* xxx = *bytes + 1;
	DEEP_LOG(INFO, OTHER, "DATA-4: %s\n", (char*) xxx);
}

int main(int argc, char** argv) {

	String str = "hello";
	DEEP_LOG(INFO, OTHER, "HELLO (construct): %s\n", str.data());

	str += " world";
	str += " world";
	if (str.length() == 17) {
		DEEP_LOG(INFO, OTHER, "HELLO (adding): success - %d, %s\n", str.length(), str.data());

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (adding): failed - %d, %s\n", str.length(), str.data());
	}

	if (str.startsWith("hello") == true) {
		DEEP_LOG(INFO, OTHER, "HELLO (start): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (start): failed\n");
	}

	if (str.endsWith("world") == true) {
		DEEP_LOG(INFO, OTHER, "HELLO (end): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (end): failed\n");
	}

	if (str.contains("world") == true) {
		DEEP_LOG(INFO, OTHER, "HELLO (contains): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (contains): failed\n");
	}

	if (str.contains("foobar") == false) {
		DEEP_LOG(INFO, OTHER, "HELLO (not contains): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (not contains): failed\n");
	}

	String newstr1 = str.replaceAll("world", "earth");
	if (newstr1.length() == 17) {
		DEEP_LOG(INFO, OTHER, "HELLO (replace): success - %d, %s\n", newstr1.length(), newstr1.data());

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (replace): failed - %d, %s\n", newstr1.length(), newstr1.data());
	}

	if (newstr1.lastIndexOf("earth") != -1) {
		DEEP_LOG(INFO, OTHER, "HELLO (index): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (index): failed\n");
	}

	String newstr2 = str.replaceLast("world", "earth");
	if (newstr2.contains("hello world earth") == true) {
		DEEP_LOG(INFO, OTHER, "HELLO (contains): success\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "HELLO (not contains): failed\n");
	}

	DEEP_LOG(INFO, OTHER, "------------------------------------- \n");

	nbyte bytes(4);
	bytes[0] = '0';
	bytes[1] = '1';
	bytes[2] = '2';
	bytes[3] = 0;
	char* aaa = bytes;
	DEEP_LOG(INFO, OTHER, "BYTES (construct): %d, %s\n", bytes.length, aaa);

	testByte1(bytes);
	testByte2(bytes);
	testByte3(&bytes);
	DEEP_LOG(INFO, OTHER, "BYTES (after tests): %d, %s\n", bytes.length, aaa);

	nbyte bytes2(4);
	bytes2[0] = 'a';
	bytes2[1] = 'b';
	bytes2[2] = 'c';
	bytes2[3] = 0;
	String bbb(&bytes2);
	DEEP_LOG(INFO, OTHER, "BYTES (stringy): %s\n", bbb.data());

	String ccc(&bytes2, 1, bytes2.length);
	DEEP_LOG(INFO, OTHER, "BYTES (stringy): %s\n", ccc.data());

	return 0;
}
