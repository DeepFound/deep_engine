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
#include <stdlib.h>
#include <assert.h>
#include <iostream>

#include "cxx/util/ArrayList.h"
#include "cxx/util/HashSet.h"
#include "cxx/util/Logger.h"
#include "cxx/util/QueueSet.h"
#include "cxx/util/concurrent/locks/UserSpaceLock.h"

#include "cxx/util/Converter.cxx"
#include "cxx/util/HashSet.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::locks;

int main(int argc, char** argv) {
	typedef QueueSet<longtype, ArrayList<longtype>, HashSet<longtype>, UserSpaceLock> QS; 
	QS queueSet((const ArrayList<longtype>&)ArrayList<longtype>(), (const HashSet<longtype>&)HashSet<longtype>(), (const UserSpaceLock&)UserSpaceLock());

	queueSet.add(9L);
	queueSet.add(1L);
	queueSet.add(8L);
	queueSet.add(9L);
	queueSet.add(2L);
	queueSet.add(6L);
	queueSet.add(3L);
	queueSet.add(4L);
	queueSet.add(7L);
	queueSet.add(1L);
	queueSet.add(0L);
	queueSet.add(5L);

	inttype size = queueSet.size();
	if (size != 10) {
		abort();
	}

	Iterator<longtype>* iter = queueSet.iterator();
	while(iter->hasNext()) {
		cout << iter->next() << endl;
		iter->remove();
		if (queueSet.size() != --size) {
			abort();
		}
	}
	Converter<Iterator<longtype>*>::destroy(iter);

	if (queueSet.size() != 0) {
		abort();
	}

	return 0;
}
