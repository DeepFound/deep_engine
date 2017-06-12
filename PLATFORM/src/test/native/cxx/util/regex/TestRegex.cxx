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
#include <stdio.h>
#include <cstdlib>

#include "cxx/lang/String.h"
#include "cxx/util/Logger.h"
#include "cxx/util/regex/Matcher.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::regex;

int test(String& regex, String& input) {
	int success = 1;
	
	Pattern pattern = Pattern::compile(regex);
	Matcher matcher = pattern.matcher(input);

	uinttype groups = matcher.groups();
	for (int i=0; i<groups; ++i) {
		success = 0;
		DEEP_LOG(INFO, OTHER, "Match (%d): %s\n", i, matcher.group(i).data());
	}
	
	return success;
}

template<typename T, int N>
FORCE_INLINE int length(T (&a)[N]) {
	return N;
}

int main(int argc, char** argv) {
	int success = 0;

	String regex[] = {
		"(12)(34)5",
		"(^|[ \t\r\n\v\f])deep_index_orientation[ \t\r\n\v\f]*=[ \t\r\n\v\f]*([a-zA-Z]*)"
	};
	
	String input[] = {
		"12345",
		"deep_index_orientation  =column"
	};

	for (int i=0; i<length(regex); ++i) {
		success |= test(regex[i], input[i]);
	}

	return success;
}
