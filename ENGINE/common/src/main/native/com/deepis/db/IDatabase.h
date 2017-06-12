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
#ifndef COM_DEEPIS_DB_IDATABASE_H_
#define COM_DEEPIS_DB_IDATABASE_H_ 

// XXX: placeholder for future interface

namespace com { namespace deepis { namespace db {

class IDatabase {

	/* TODO

	//
	// Object Coordination
	//
	public ITransaction newTransaction();

	public void deleteTransaction(ITransaction transaction);

	public <T> T executeWorkflow(IWorkflow<T> workflow) throws Exception;

	public <T> T executeSynchronize(ISynchronize<T> synchronize) throws Exception;

	//
	// Object Access / Modification
	//
	public <T> T newObject(Class dataStore, String key, Class classImpl, Object... arguments);

	public <T> T putObject(Class dataStore, String key, Object object);

	public void putObjects(Class dataStore, Map<String,Object> map);

	public boolean containsObject(Class dataStore, String key);

	public <T> Collection<String> getKeys(Class<T> dataStore);

	public <T> T getObject(Class dataStore, String key);

	public <T> Collection<T> getObjects(Class<T> dataStore);

	public <T> Collection<T> getObjects(Class<T> dataStore, Collection<String> keys);

	public void deleteObject(Class dataStore, Object object);

	public void deleteObject(Class dataStore, String key);

	public void deleteObjects(Class dataStore);

	public long numberOfObjects(Class dataStore);

	public void close();

	*/
};

} } } // namespace

#endif /*COM_DEEPIS_DB_IDATABASE_H_*/
