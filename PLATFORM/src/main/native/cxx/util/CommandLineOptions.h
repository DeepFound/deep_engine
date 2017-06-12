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
#ifndef COM_DEEPIS_CORE_UTIL_COMMANDLINEOPTIONS_H_
#define COM_DEEPIS_CORE_UTIL_COMMANDLINEOPTIONS_H_

#include "cxx/lang/String.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "cxx/util/HashMap.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace com { namespace deepis { namespace core { namespace util {

class CommandLineOptions {
	private:
		HashMap<String*, String*> m_options;

	public:
		CommandLineOptions(int argc, char** argv) :
			m_options(100, true, true) {

			if ((argc % 2) != 0) {
				for (int i=1; i<argc; i+=2) {
					if (strlen(argv[i]) > 1) {
						if (strncmp(argv[i], "-", 1) == 0) {
							m_options.put(new String(argv[i]), new String(argv[i + 1]));
						}
					}
				}

			} else {
				throw new UnsupportedOperationException("Invalid number of command line args");
			}
		}

		const char* getString(const char* option, const char* defaultValue="") {
			String opt(option);

			String* value = m_options.get(&opt);
			if (value != null) {
				return value->c_str();

			} else {
				return defaultValue;
			}
		}

		int getInteger(const char* option, int defaultValue=-1) {
			String opt(option);

			String* value = m_options.get(&opt);
			if (value != null) {
				char* end;
				return (int)strtol(value->c_str(), &end, 10);

			} else {
				return defaultValue;
			}
		}
};

} } } } // namespace

#endif /*COM_DEEPIS_CORE_UTIL_COMMANDLINEOPTIONS_H_*/
