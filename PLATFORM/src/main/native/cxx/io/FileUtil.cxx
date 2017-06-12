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
#include <sys/stat.h>

#include <unistd.h>

#include "cxx/io/FileUtil.h"

using namespace com::deepis::core::util;

boolean FileUtil::mkdir(const char* directory) {
	return ::mkdir(directory, 0) == 0;
}

boolean FileUtil::mkdir(const File* directory) {
	return mkdir(directory->getPath());
}

boolean FileUtil::mkdir(const String& directory) {
	return mkdir(directory);
}

boolean FileUtil::mkdirs(const char* directory) {
	return File::mkdirs(directory, 0);
}

boolean FileUtil::mkdirs(const File* directory) {
	return File::mkdirs(directory->getPath(), 0);
}

boolean FileUtil::mkdirs(const String& directory) {
	return File::mkdirs(directory, 0);
}

boolean FileUtil::exists(const char* fileName) {
	struct stat buffer;   
	return (stat(fileName, &buffer) == 0);
}

boolean FileUtil::exists(const String& fileName) {
	return exists(fileName.data());
}

boolean FileUtil::clobber(const char* fileName, const boolean followSymlinks) {
	if ((followSymlinks == true) && (Files::isSymbolicLink(fileName) == true)) {
		String target = Files::readSymbolicLink(fileName);
		if ((target.length() == 0) || (::remove((const char*)target) != 0)) {
			return false;
		}
	}

	boolean status = (::remove(fileName) == 0);

	// XXX: there are times where ::remove succeeds and status is incorrect
	if (status == false) {
		status = access(fileName, F_OK) != -1;
	}

	return status;
}

boolean FileUtil::clobber(const File* fileObject, const boolean followSymlinks) {
	return clobber((const char*) fileObject->getPath(), followSymlinks);
}

boolean FileUtil::clobber(const String& fileName, const boolean followSymlinks) {
	return clobber((const char*) fileName.data(), followSymlinks);
}

ArrayList<String*>* FileUtil::list(const char* directory) {
	File dir(directory);
	return dir.list();
}

ArrayList<String*>* FileUtil::list(const File* directory) {
	return list((const char*) directory->getPath());
}

ArrayList<String*>* FileUtil::list(const String& directory) {
	return list((const char*) directory.data());
}

boolean FileUtil::move(const char* sourceName, const char* targetName) {
	return ::rename(sourceName, targetName) == 0;
}

boolean FileUtil::move(const File* sourceFile, const File* targetFile) {
	return move((const char*) sourceFile->getPath(), (const char*) targetFile->getPath());
}

boolean FileUtil::move(const String& sourceFile, const String& targetFile) {
	return move((const char*) sourceFile.data(), (const char*) targetFile.data());
}
