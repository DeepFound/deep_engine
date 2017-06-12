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
#include "cxx/util/regex/Pattern.h"

#ifndef CXX_UTIL_REGEX_MATCHER_H_
#define CXX_UTIL_REGEX_MATCHER_H_

using namespace std;
using namespace cxx::lang;

namespace cxx { namespace util { namespace regex {

class Matcher : public Object {
	private:
		static const inttype MAX_GROUPS = 16;

		Pattern& m_pattern;
		String m_input;
		regmatch_t match[MAX_GROUPS];

		FORCE_INLINE void exec() {
			for(int i=0; i<MAX_GROUPS; ++i) {
				match[i].rm_so = -1;
			}
			regexec(&m_pattern.m_regex, m_input.data(), MAX_GROUPS, match, 0);
		}

		Matcher(Pattern& pattern, String& input) :
			m_pattern(pattern),
			m_input(input) {
			exec();
		}
	public:
		Matcher(const Matcher& matcher) :
			m_pattern(matcher.m_pattern),
			m_input(matcher.m_input) {
			exec();
		}
		virtual ~Matcher() {
			// XXX: nothing to do
		}

		FORCE_INLINE boolean matches() {
			return match[0].rm_so >= 0;
		}

		FORCE_INLINE inttype groups() {
			inttype i=0;
			for(; i<MAX_GROUPS; ++i) {
				if(match[i].rm_so < 0)
					break;
			}
			return i;
		}

		FORCE_INLINE String group(inttype i) {
			if (i >= MAX_GROUPS) {
				throw RuntimeException("Invalid group index");
			}
			return m_input.substring(match[i].rm_so, match[i].rm_eo - match[i].rm_so);
		}

		friend class Pattern;
};

} } } // namespace

#endif /*CXX_UTIL_REGEX_MATCHER_H_*/
