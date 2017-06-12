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

#include <errno.h>
#include <sys/stat.h>

#include <unistd.h>
#include <dirent.h>

#include "cxx/io/File.h"

using namespace cxx::io;

const String File::separator("/");
const char File::separatorChar = '/';

boolean File::mkdirs(const char* dir, int mode) {
	const char* path = dir + 1;
	while (*path) {
		if (*path == '/') {
			*((char*) path) = '\0';
			if ((::mkdir(dir, mode) == -1) && (errno != EEXIST)) {
				return false;
			}

			*((char*) path) = '/';
		}

		path++;
	}

	if (*(path - 1) != '/') {
		if ((::mkdir(dir, mode) == -1) && (errno != EEXIST)) {
			return false;
		}
	}

	return true;
}

File::File(const char* pathname):
	String(pathname) {
}

File::File(const String& pathname):
	String(pathname) {
}

const char* File::getName() const {
	// TODO(NAME: NO PATH)
	return null;
}

const char* File::getPath() const {
	return data();
}

long long File::length() const {
	struct stat st;
	stat(data(), &st);

	return st.st_size;
}

boolean File::exists() const {
	return access(data(), F_OK) != -1;
}

boolean File::mkdir() {
	return ::mkdir(getPath(), 0) == 0;
}

boolean File::mkdirs() {
	return mkdirs(getPath(), 0);
}

boolean File::renameTo(const char* dest) {
	boolean result = ::rename(getPath(), dest) == 0;
	if (result == true) {
		*this = dest;
	}

	return result;
}

ArrayList<String*>* File::list(ArrayList<String*>* list) const {
	if (list == null) {
		list = new ArrayList<String*>(4 /* ArrayList::INITIAL_CAPACITY */, true);
	}

	struct dirent* file;
	DIR* dir = opendir(getPath());
	if (dir == null) {
		return null;
	}

	while ((file = readdir(dir)) != NULL) {
		list->add(new String((const char*) file->d_name));
	}

	closedir(dir);

	return list;
}

boolean File::clobber() {
	return ::remove(data());
}

File File::getParentFile() {
	inttype index = lastIndexOf(File::separator);
	if (index == -1) {
		return File(".");
	}

	return File(substring(0, index));
}
