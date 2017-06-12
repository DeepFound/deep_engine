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
#ifndef CXX_LANG_STRING_H_
#define CXX_LANG_STRING_H_

#include <string>
#include <sstream>
#include <string.h>

#include "cxx/lang/nbyte.h"
#include "cxx/lang/Object.h"
#include "cxx/lang/Comparable.h"

using namespace std;

namespace cxx { namespace lang {

class String : public string, public Comparable {

	private:
		long m_hash;

	private:
		inline void _replace_(size_t pos1, size_t n1, const char* s) {
			string::replace(pos1, n1, s);
		}

		inline void _replace_(size_t pos1, size_t n1, const String& str) {
			string::replace(pos1, n1, str);
		}

		inline void _replace_(size_t pos1, size_t n1, size_t n2, char c) {
			string::replace(pos1, n1, n2, c);
		}

	public:
		template<class T>
		inline static String valueOf(T t) {
			stringstream stream;
			stream << t;
			return stream.str();
		}

	public:
		inline String(void):
			m_hash(0) {
		}

		inline String(const char* data):
			string(data),
			m_hash(0) {
		}

		inline String(const string& data):
			string(data),
			m_hash(0) {
		}

		inline String(const String& data):
			string(data),
			m_hash(0) {
		}

		inline String(const char* data, long length):
			string(data, length),
			m_hash(0) {
		}

		inline String(const nbyte* bytes):
			string(*bytes, bytes->length),
			m_hash(0) {
		}

		inline String(const nbyte* bytes, int offset, int length):
			string(*bytes + offset, length - offset),
			m_hash(0) {
		}

		virtual ~String() {
		}

		inline int length() const {
			return size();
		}

		inline char charAt(int i) const {
			return at(i);
		}

		inline boolean contains(char s) const {
			int pos = find(s);
			return (pos >= 0) ? true : false;
		}

		inline boolean contains(const char* s) const {
			int pos = find(s);
			return (pos >= 0) ? true : false;
		}

		inline boolean contains(const String& s) const {
			int pos = find(s);
			return (pos >= 0) ? true : false;
		}

		inline int indexOf(char s) const {
			int pos = find(s);
			return (pos >= 0) ? pos : -1;
		}

		inline int indexOf(const char* s) const {
			int pos = find(s);
			return (pos >= 0) ? pos : -1;
		}

		inline int indexOf(const String& s) const {
			int pos = find(s);
			return (pos >= 0) ? pos : -1;
		}

		inline int lastIndexOf(char s) const {
			int pos = rfind(s);
			return (pos >= 0) ? pos : -1;
		}

		inline int lastIndexOf(const char* s) const {
			int pos = rfind(s);
			return (pos >= 0) ? pos : -1;
		}

		inline int lastIndexOf(const String& s) const {
			int pos = rfind(s);
			return (pos >= 0) ? pos : -1;
		}

		inline boolean startsWith(char s) const {
			return indexOf(s) == 0;
		}

		inline boolean startsWith(const char* s) const {
			return indexOf(s) == 0;
		}

		inline boolean startsWith(const String& s) const {
			return indexOf(s) == 0;
		}

		inline boolean endsWith(char s) const {
			int pos = lastIndexOf(s);
			return (pos != -1) && (pos == (length() - 1));
		}

		inline boolean endsWith(const char* s) const {
			int pos = lastIndexOf(s);
			return (pos != -1) && (pos == (length() - (int) strlen(s)));
		}

		inline boolean endsWith(const String& s) const {
			int pos = lastIndexOf(s);
			return (pos != -1) && (pos == (length() - s.length()));
		}

		inline String replace(char s1, char s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			if (index != -1) {
				copy._replace_(index, 1, 1, s2);
			}

			return copy;
		}

		inline String replace(const char* s1, const char* s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			if (index != -1) {
				copy._replace_(index, strlen(s1), s2);
			}

			return copy;
		}

		inline String replace(const String& s1, const String& s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			if (index != -1) {
				copy._replace_(index, s1.length(), s2);
			}

			return copy;
		}

		inline String replaceAll(char s1, char s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			while (index != -1) {
				copy._replace_(index, 1, 1, s2);
				index = copy.indexOf(s1);
			}

			return copy;
		}

		inline String replaceAll(const char* s1, const char* s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			while (index != -1) {
				copy._replace_(index, strlen(s1), s2);
				index = copy.indexOf(s1);
			}

			return copy;
		}

		inline String replaceAll(const String& s1, const String& s2) {
			String copy = String(*this);
			int index = copy.indexOf(s1);
			while (index != -1) {
				copy._replace_(index, s1.length(), s2);
				index = copy.indexOf(s1);
			}

			return copy;
		}

		inline String replaceLast(const String& s1, const String& s2) {
			int pos = lastIndexOf(s1);
    			if (pos > -1) {
        			return substring(0, pos) + s2 + substring(pos + s1.length(), length());

			} else {
        			return *this;
    			}
		}

		inline String substring(int pos = 0, int n = -1) const {
			if (n == -1) {
				n = length();
			}

			return String(substr(pos, n));
		}

		inline operator const char*(void) const {
			return data();
		}

		inline nbyte* getBytes() const {
			return new nbyte((bytearray) data(), length());
		}

		virtual Object* clone() const {
			return new String(*this);
		}

		virtual long hashCode(void) const {
			long hash = m_hash;
			int length = size();
			if ((hash == 0) && (length > 0)) {
				long offset = 0;
				const char* val = data();
				for (int i = 0; i < length; i++) {
					hash = 31 * hash + val[offset++];
				}

				((String*) this)->m_hash = hash;
			}

			return hash;
		}

		virtual boolean equals(const Object* obj) const {
			return obj != null && compareTo(obj) == 0;
		}

		virtual int compareTo(const Object* obj) const {
			return compare((String&) *obj);
		}
};

} } // namespace

#endif /*CXX_LANG_STRING_H_*/
