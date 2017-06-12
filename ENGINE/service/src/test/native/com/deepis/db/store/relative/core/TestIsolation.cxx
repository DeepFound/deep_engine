#include "TestClient.h"

inttype key_1 = 1,
	key_2 = 2,
	key_3 = 3,
	key_4 = 4;

inttype val_1 = 1,
	val_2 = 2,
	val_3 = 3,
	val_4 = 4;

static RealTimeMap<inttype>* MAP_PRIMARY = null;
static RealTimeMap<inttype>* MAP_SECONDARY = null;

static Comparator<inttype>* COMPARATOR = null;
static KeyBuilder<inttype>* KEY_BUILDER = null;

void startup();
void shutdown();
void clear();

void testReadUncommitted();
void testReadCommitted();
void testRepeatableRead();
void testSerializable();

void testSecondaryUpdateRollback();
void testSecondaryUpdateRollbackRepeatable();
void testRemoveRepeatable();

inttype main(inttype argc, char** argv) {
	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	startup();

	testReadUncommitted();
	testReadCommitted();
	testRepeatableRead();
	testSerializable();

	//testSecondaryUpdateRollback();
	//testSecondaryUpdateRollbackRepeatable();

	testRemoveRepeatable();

	shutdown();

	return 0;
}

void testReadUncommitted() {
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::UNCOMMITTED);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::UNCOMMITTED);

	client1.start();
	client2.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 1\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 2\n");
	client2.verifyPrimary(key_1, true);
	client2.verifySecondary(key_1, true);

	// client1: begin
	client1.begin();

	// client2: remove
	client2.begin();
	client2.remove(key_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 3\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 4\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	// client2: commit
	client2.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 5\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 6\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	// client1: commit
	client1.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 7\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - 8\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	client1.stop();
	client2.stop();

	DEEP_LOG(INFO, OTHER, "testReadUncommitted() - SUCCESS\n");
}

void testReadCommitted() {
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::COMMITTED);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::COMMITTED);

	client1.start();
	client2.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 1\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 2\n");
	client2.verifyPrimary(key_1, true);
	client2.verifySecondary(key_1, true);

	// client1: begin
	client1.begin();

	// client2: remove
	client2.begin();
	client2.remove(key_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 3\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 4\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	// client2: commit
	client2.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 5\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 6\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	// client1: commit
	client1.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 7\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testReadCommitted() - 8\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	client1.stop();
	client2.stop();

	DEEP_LOG(INFO, OTHER, "testReadCommitted() - SUCCESS\n");
}

void testRepeatableRead() {
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	TestClient client3(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);

	client1.start();
	client2.start();
	client3.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: begin
	client1.begin();

	// client1: create a repeatable get
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 1\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client2: remove
	client2.begin();
	client2.remove(key_1);

	// client2: verify removal
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 2\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	// client3: committed get
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 3\n");
	client3.verifyPrimary(key_1, true);
	client3.verifySecondary(key_1, true);

	// client2: commit
	client2.commit();

	// client3: verify
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 4\n");
	client3.verifyPrimary(key_1, false);
	client3.verifySecondary(key_1, false);

	// client1: verify repeatable
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 5\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client1: commit
	client1.commit();

	// client1: verify removal
	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - 6\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	client1.stop();
	client2.stop();
	client3.stop();

	DEEP_LOG(INFO, OTHER, "testRepeatableRead() - SUCCESS\n");
}

void testSerializable() {
	DEEP_LOG(INFO, OTHER, "testSerializable() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::SERIALIZABLE);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::SERIALIZABLE);
	TestClient client3(MAP_PRIMARY, MAP_SECONDARY, Transaction::SERIALIZABLE);

	client1.start();
	client2.start();
	client3.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: begin
	client1.begin();

	// client1: acquire read lock
	DEEP_LOG(INFO, OTHER, "testSerializable() - 1\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client2: remove should fail (timeout)
	client2.begin();
	if (client2.remove(key_1) == true) {
		DEEP_LOG(ERROR, OTHER, "client2: remove key %d succeeded, should have timed out\n", key_1);
	}

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 2\n");
	client2.verifyPrimary(key_1, true);
	client2.verifySecondary(key_1, true);

	// client3: committed get
	DEEP_LOG(INFO, OTHER, "testSerializable() - 3\n");
	client3.verifyPrimary(key_1, true);
	client3.verifySecondary(key_1, true);

	// client2: commit
	client2.commit();

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 4\n");
	client3.verifyPrimary(key_1, true);
	client3.verifySecondary(key_1, true);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 5\n");
	client1.verifyPrimary(key_1, true);
	client1.verifySecondary(key_1, true);

	// client1: remove
	client1.remove(key_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 6\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client1: commit
	client1.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 7\n");
	client1.verifyPrimary(key_1, false);
	client1.verifySecondary(key_1, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testSerializable() - 8\n");
	client2.verifyPrimary(key_1, false);
	client2.verifySecondary(key_1, false);

	client1.stop();
	client2.stop();
	client3.stop();

	DEEP_LOG(INFO, OTHER, "testSerializable() - SUCCESS\n");
}

void testSecondaryUpdateRollback() {
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	client1.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: create a previous secondary story (key 2)
	client1.put(key_2, val_2);
	client1.remove(key_2);

	// client1: create a previous secondary story (key 3)
	client1.put(key_3, val_3);
	client1.remove(key_3);

	// client1: begin
	client1.begin();

	// client1: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - 1\n");
	client1.verifySecondary(key_1, true);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, false);

	// client1: update to change secondary (key 2)
	client1.update(key_1, val_2);

	// client1: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - 2\n");
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, false);

	// client1: update to change secondary again (key 3)
	client1.update(key_1, val_3);

	// client1: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - 3\n");
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, true);

	// client1: update to change secondary again (key 4)
	client1.update(key_1, val_4);

	// client1: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - 4\n");
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, false);
	client1.verifySecondary(key_4, true);

	// client1: rollback
	client1.rollback();

	// client1: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - 5\n");
	client1.verifySecondary(key_1, true);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, false);
	client1.verifySecondary(key_4, false);

	client1.stop();

	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollback() - SUCCESS\n");
}

void testSecondaryUpdateRollbackRepeatable() {
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);

	client1.start();
	client2.start();

	// client1: committed put
	client1.put(key_1, val_1);

	// client1: create a previous secondary story (key 2)
	client1.put(key_2, val_2);
	client1.remove(key_2);

	// client1: create a previous secondary story (key 3)
	client1.put(key_3, val_3);
	client1.remove(key_3);

	// client1: begin
	client1.begin();

	// client2: begin
	client2.begin();

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - 1\n");
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, false);

	// client1: update to change secondary (key 2)
	client1.update(key_1, val_2);

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - 2\n");
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, false);

	// client1: update to change secondary again (key 3)
	client1.update(key_1, val_3);

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - 3\n");
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, false);

	// client1: update to change secondary again (key 4)
	client1.update(key_1, val_4);

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - 4\n");
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, false);
	client2.verifySecondary(key_4, false);

	// client1: rollback
	client1.rollback();

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - 5\n");
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, false);
	client2.verifySecondary(key_4, false);

	client1.stop();
	client2.stop();

	DEEP_LOG(INFO, OTHER, "testSecondaryUpdateRollbackRepeatable() - SUCCESS\n");
}

void testRemoveRepeatable() {
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - BEGIN\n");

	clear();
	
	TestClient client1(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	TestClient client2(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);
	TestClient client3(MAP_PRIMARY, MAP_SECONDARY, Transaction::REPEATABLE);

	client1.start();
	client2.start();
	client3.start();

	// client1: committed put
	client1.put(key_1, val_1);
	client1.put(key_2, val_2);
	client1.put(key_3, val_3);

	client1.begin();
	client2.begin();
	client3.begin();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 1\n");
	client1.verifyPrimary(key_1, true);
	client1.verifyPrimary(key_2, true);
	client1.verifyPrimary(key_3, true);
	client1.verifySecondary(key_1, true);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, true);

	// client1: remove
	client1.remove(key_1);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 2\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, true);
	client1.verifyPrimary(key_3, true);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, true);

	// client2: verify secondaries
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 3\n");
	client2.verifyPrimary(key_1, true);
	client2.verifyPrimary(key_2, true);
	client2.verifyPrimary(key_3, true);
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, true);
	client2.verifySecondary(key_3, true);

	client2.remove(key_2);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 4\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, true);
	client1.verifyPrimary(key_3, true);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, true);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 5\n");
	client2.verifyPrimary(key_1, true);
	client2.verifyPrimary(key_2, false);
	client2.verifyPrimary(key_3, true);
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, true);

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 6\n");
	client3.verifyPrimary(key_1, true);
	client3.verifyPrimary(key_2, true);
	client3.verifyPrimary(key_3, true);
	client3.verifySecondary(key_1, true);
	client3.verifySecondary(key_2, true);
	client3.verifySecondary(key_3, true);

	client3.put(key_4, val_4);

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 7\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, true);
	client1.verifyPrimary(key_3, true);
	client1.verifyPrimary(key_4, false);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, true);
	client1.verifySecondary(key_4, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 8\n");
	client2.verifyPrimary(key_1, true);
	client2.verifyPrimary(key_2, false);
	client2.verifyPrimary(key_3, true);
	client2.verifyPrimary(key_4, false);
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, true);
	client2.verifySecondary(key_4, false);

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 9\n");
	client3.verifyPrimary(key_1, true);
	client3.verifyPrimary(key_2, true);
	client3.verifyPrimary(key_3, true);
	client3.verifyPrimary(key_4, true);
	client3.verifySecondary(key_1, true);
	client3.verifySecondary(key_2, true);
	client3.verifySecondary(key_3, true);
	client3.verifySecondary(key_4, true);

	client1.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 10\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, true);
	client1.verifyPrimary(key_3, true);
	client1.verifyPrimary(key_4, false);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, true);
	client1.verifySecondary(key_3, true);
	client1.verifySecondary(key_4, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 11\n");
	client2.verifyPrimary(key_1, true);
	client2.verifyPrimary(key_2, false);
	client2.verifyPrimary(key_3, true);
	client2.verifyPrimary(key_4, false);
	client2.verifySecondary(key_1, true);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, true);
	client2.verifySecondary(key_4, false);

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 12\n");
	client3.verifyPrimary(key_1, true);
	client3.verifyPrimary(key_2, true);
	client3.verifyPrimary(key_3, true);
	client3.verifyPrimary(key_4, true);
	client3.verifySecondary(key_1, true);
	client3.verifySecondary(key_2, true);
	client3.verifySecondary(key_3, true);
	client3.verifySecondary(key_4, true);

	client2.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 13\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, false);
	client1.verifyPrimary(key_3, true);
	client1.verifyPrimary(key_4, false);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, true);
	client1.verifySecondary(key_4, false);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 14\n");
	client2.verifyPrimary(key_1, false);
	client2.verifyPrimary(key_2, false);
	client2.verifyPrimary(key_3, true);
	client2.verifyPrimary(key_4, false);
	client2.verifySecondary(key_1, false);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, true);
	client2.verifySecondary(key_4, false);

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 15\n");
	client3.verifyPrimary(key_1, true);
	client3.verifyPrimary(key_2, true);
	client3.verifyPrimary(key_3, true);
	client3.verifyPrimary(key_4, true);
	client3.verifySecondary(key_1, true);
	client3.verifySecondary(key_2, true);
	client3.verifySecondary(key_3, true);
	client3.verifySecondary(key_4, true);

	client3.commit();

	// client1: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 16\n");
	client1.verifyPrimary(key_1, false);
	client1.verifyPrimary(key_2, false);
	client1.verifyPrimary(key_3, true);
	client1.verifyPrimary(key_4, true);
	client1.verifySecondary(key_1, false);
	client1.verifySecondary(key_2, false);
	client1.verifySecondary(key_3, true);
	client1.verifySecondary(key_4, true);

	// client2: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 17\n");
	client2.verifyPrimary(key_1, false);
	client2.verifyPrimary(key_2, false);
	client2.verifyPrimary(key_3, true);
	client2.verifyPrimary(key_4, true);
	client2.verifySecondary(key_1, false);
	client2.verifySecondary(key_2, false);
	client2.verifySecondary(key_3, true);
	client2.verifySecondary(key_4, true);

	// client3: verify keys
	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - 18\n");
	client3.verifyPrimary(key_1, false);
	client3.verifyPrimary(key_2, false);
	client3.verifyPrimary(key_3, true);
	client3.verifyPrimary(key_4, true);
	client3.verifySecondary(key_1, false);
	client3.verifySecondary(key_2, false);
	client3.verifySecondary(key_3, true);
	client3.verifySecondary(key_4, true);

	client1.stop();
	client2.stop();
	client3.stop();

	DEEP_LOG(INFO, OTHER, "testRemoveRepeatable() - SUCCESS\n");
}

void clear() {
	MAP_SECONDARY->unmount();

	MAP_PRIMARY->clear();

	MAP_SECONDARY->mount();
}

void startup() {
	longtype options = RealTimeMap<inttype>::O_CREATE | RealTimeMap<inttype>::O_DELETE | RealTimeMap<inttype>::O_FIXEDKEY;

	Properties::setTransactionTimeout(1000 /* msecs */);

	MAP_PRIMARY = new RealTimeMap<inttype>("./datastore", options, sizeof(inttype), sizeof(inttype));

	COMPARATOR = new Comparator<inttype>();

	KEY_BUILDER = new KeyBuilder<inttype>();
	KEY_BUILDER->setOffset(0);

	MAP_SECONDARY = new RealTimeMap<inttype>("./secondary.datastore", options, sizeof(inttype), sizeof(inttype), COMPARATOR, KEY_BUILDER);

	MAP_PRIMARY->associate(MAP_SECONDARY, false);

	MAP_PRIMARY->mount();

	MAP_SECONDARY->mount();
	MAP_PRIMARY->recover(false);
}

void shutdown() {
	MAP_PRIMARY->unmount(false);
	MAP_SECONDARY->unmount(false);

	delete MAP_SECONDARY;
	MAP_SECONDARY = null;

	delete MAP_PRIMARY;
	MAP_PRIMARY = null;

	delete COMPARATOR;
	COMPARATOR = null;

	delete KEY_BUILDER;
	KEY_BUILDER = null;
}

