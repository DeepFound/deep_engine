#include "cxx/lang/System.h"
#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace com::deepis::db::store::relative::core;
using namespace com::deepis::db::store::relative::util;

uinttype BUFFER_SIZE = 1000;
ushorttype MULTIPLIER = 3;
ushorttype CHUNK = 100;

File* file = null;
BufferedRandomAccessFile* braFile = null;
RealTimeMap<int>* MAP = null;

static Comparator<int>* COMPARATOR = null;
static KeyBuilder<int>* KEY_BUILDER = null;

void caseNoCompression();
void caseCompression();
void caseMultipleCompressionBlocks();
void caseMixed1();
void caseMixed2();
void caseMixedMultipleRead();
void caseMixedWithoutBlock();
void casePrimitiveWrites();
void caseStartupShutdown();
void caseStartupShutdownWithDelFirstKey();
void caseStartupShutdownWithDelKey();
void caseStartupShutdownDelAll();
void caseBigRead();
void caseUnevenFile();

void caseCompressionLargeAlloc();

longtype  writeData();
void readData(longtype position);

void initFile();
void startup(boolean del, boolean varValues = false);
void shutdown();

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	caseNoCompression();
	caseCompression();
	caseMultipleCompressionBlocks();

	caseMixed1();
	caseMixed2();
	caseMixedMultipleRead();
	caseMixedWithoutBlock();

	casePrimitiveWrites();

	caseStartupShutdown();

	caseStartupShutdownWithDelKey();
	caseStartupShutdownDelAll();
	caseStartupShutdownWithDelFirstKey();

	caseBigRead();
	caseUnevenFile();

	caseCompressionLargeAlloc();

	return 0;
}

void caseNoCompression() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - NO COMPRESSION\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING DATA WITHOUT COMPRESSION - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);
        longtype location = writeData();
        braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA WITHOUT COMPRESSION\n");
        readData(location);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

        file->clobber();
        delete braFile;
	delete file;
}

void caseCompression() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - SIMPLE COMPRESSION\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING DATA WITH COMPRESSION - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype location = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA WITH COMPRESSION\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);

        readData(location);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

        file->clobber();
        delete braFile;
	delete file;
}

void caseMultipleCompressionBlocks() {

        DEEP_LOG(INFO, OTHER, " TEST CASE - MULTIPLE COMPRESSION BLOCKS\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING DATA BLOCK 1 WITH COMPRESSION - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype loc1 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        DEEP_LOG(INFO, OTHER, " WRITING DATA BLOCK 2 WITH COMPRESSION - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype loc2 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA BLOCK 1 WITH COMPRESSION\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(loc1);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA BLOCK 2 WITH COMPRESSION\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(loc2);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

        file->clobber();
        delete braFile;
        delete file;
}

void caseMixed1() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - MIXED DATA 1\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING MIXED DATA - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);
        longtype pos1 = writeData();
        braFile->flush();

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos2 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        longtype pos3 = writeData();
        braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING MIXED DATA\n");
        readData(pos1);
        
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos2);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

	readData(pos3);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	file->clobber();
	delete braFile;
	delete file;
}

void caseMixed2() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - MIXED DATA 2\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING MIXED DATA - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos1 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        longtype pos2 = writeData();
	braFile->flush();

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos3 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING MIXED DATA\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos1);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
        
        readData(pos2);

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos3);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	file->clobber();
	delete braFile;
	delete file;
}

void caseMixedMultipleRead() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - MIXED DATA MULTIPLE READ\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING MIXED DATA - %d BYTES WITH %d BYTE BUFFER\n", BUFFER_SIZE * MULTIPLIER, BUFFER_SIZE);

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos1 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        longtype pos2 = writeData();
	braFile->flush();

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos3 = writeData();
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

        DEEP_LOG(INFO, OTHER, " VERIFYING MIXED DATA (PASS 1)\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos1);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
        
        readData(pos2);

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos3);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " VERIFYING MIXED DATA (PASS 2)\n");
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos1);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
        
        readData(pos2);

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        readData(pos3);
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	file->clobber();
	delete braFile;
	delete file;
}

void caseMixedWithoutBlock() {
	
	DEEP_LOG(INFO, OTHER, " TEST CASE - MIXED WITHOUT BLOCK\n");
	initFile();

        DEEP_LOG(INFO, OTHER, " WRITING MIXED DATA\n");
	
	longtype pos0 = braFile->getPosition(); 
	braFile->writeInt(1); 
	braFile->writeInt(2); 
	braFile->writeInt(3);	
	braFile->flush();

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
        longtype pos1 = writeData(); 
	//need this id there is no compression used here: braFile->flush();
	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
	braFile->flush();

	braFile->seek(pos0); 
        DEEP_LOG(INFO, OTHER, " VERIFYING MIXED DATA WITHOUT BLOCK\n");

	int a = braFile->readInt();
	int b = braFile->readInt();
	int c = braFile->readInt();

	if (a != 1 || b!= 2 || c != 3) {
		DEEP_LOG(ERROR, OTHER, "FAILED READ EXPECTED: [1 2 3] GOT: [%d %d %d]\n", a, b, c);
		exit(-1);
	}

	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
	braFile->seek(pos1);
	readData(-1);
	braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

        file->clobber();
        delete braFile;
        delete file;
}

void casePrimitiveWrites() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - PRIMITIVE WRITES\n");
        initFile();

        DEEP_LOG(INFO, OTHER, " WRITING SOME INTS\n");

        longtype pos0 = braFile->getPosition();

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);

        braFile->writeInt(1);
        braFile->writeInt(2);
        braFile->writeInt(3);

        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
        braFile->flush();

	
        braFile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
        braFile->seek(pos0);
        DEEP_LOG(INFO, OTHER, " VERIFYING PRIMITIVE WRITES\n");

        int a = braFile->readInt();
        int b = braFile->readInt();
        int c = braFile->readInt();

        if (a != 1 || b!= 2 || c != 3) {
                DEEP_LOG(ERROR, OTHER, "FAILED READ EXPECTED: [1 2 3] GOT: [%d %d %d]\n", a, b, c);
                exit(-1);
        }

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");

        file->clobber();
        delete braFile;
        delete file;

}

void caseStartupShutdown() {

	DEEP_LOG(INFO, OTHER, " TEST CASE - STARTUP SHUTDOWN MAP\n");
	startup(true);
	
	Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);
	
	nbyte data(sizeof(int));
	MAP->put(1, &data, RealTimeMap<int>::UNIQUE, tx);
	MAP->put(2, &data, RealTimeMap<int>::UNIQUE, tx);
	MAP->put(3, &data, RealTimeMap<int>::UNIQUE, tx);

	tx->commit(tx->getLevel());
        Transaction::destroy(tx);

	shutdown();
	DEEP_LOG(INFO, OTHER, " SLEEPING 3 SECONDS FOR SHUTDOWN...\n");
	Thread::sleep(3000);
	startup(false);

	tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

	int retKey;
	if (MAP->get(1, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [1] GOT NOTHING\n");
		exit(-1);
	}
	if (MAP->get(2, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [2] GOT NOTHING\n");
		exit(-1);
	}
	if (MAP->get(3, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [3] GOT NOTHING\n");
		exit(-1);
	}

	tx->commit(tx->getLevel());
        Transaction::destroy(tx);

	shutdown();

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");
}

void caseStartupShutdownWithDelFirstKey() {

        DEEP_LOG(INFO, OTHER, " TEST CASE - STARTUP SHUTDOWN MAP WITH DELETED FIRST KEY\n");
        startup(true);

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        nbyte data(sizeof(int));
        MAP->put(1, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(2, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(3, &data, RealTimeMap<int>::UNIQUE, tx);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        MAP->remove(1, &data, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();
        DEEP_LOG(INFO, OTHER, " SLEEPING 3 SECONDS FOR SHUTDOWN...\n");
        Thread::sleep(3000);
        startup(false);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        int retKey;
        if (MAP->get(1, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: NOTHING GOT [1]\n");
                exit(-1);
        }
        if (MAP->get(2, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [2] GOT NOTHING\n");
                exit(-1);
        }
        if (MAP->get(3, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [3] GOT NOTHING\n");
                exit(-1);
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");
}

void caseStartupShutdownWithDelKey() {

        DEEP_LOG(INFO, OTHER, " TEST CASE - STARTUP SHUTDOWN MAP WITH DELETED KEY\n");
        startup(true);

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        nbyte data(sizeof(int));
        MAP->put(1, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(2, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(3, &data, RealTimeMap<int>::UNIQUE, tx);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        MAP->remove(2, &data, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();
        DEEP_LOG(INFO, OTHER, " SLEEPING 3 SECONDS FOR SHUTDOWN...\n");
        Thread::sleep(3000);
        startup(false);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        int retKey;
        if (MAP->get(1, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [1] GOT NOTHING\n");
                exit(-1);
        }
        if (MAP->get(2, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: NOTHING GOT [2]\n");
                exit(-1);
        }
        if (MAP->get(3, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [3] GOT NOTHING\n");
                exit(-1);
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");
}


void caseStartupShutdownDelAll() {

        DEEP_LOG(INFO, OTHER, " TEST CASE - STARTUP SHUTDOWN MAP WITH DELETE ALL\n");
        startup(true);

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        nbyte data(sizeof(int));
        MAP->put(1, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(2, &data, RealTimeMap<int>::UNIQUE, tx);
        MAP->put(3, &data, RealTimeMap<int>::UNIQUE, tx);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        MAP->remove(1, &data, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);
        MAP->remove(2, &data, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);
        MAP->remove(3, &data, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();
        DEEP_LOG(INFO, OTHER, " SLEEPING 3 SECONDS FOR SHUTDOWN...\n");
        Thread::sleep(3000);
        startup(false);

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        int retKey;
        if (MAP->get(1, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: NOTHING GOT [1]\n");
                exit(-1);
        }
        if (MAP->get(2, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: NOTHING GOT [2]\n");
                exit(-1);
        }
        if (MAP->get(3, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true) {
                DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: NOTHING GOT [3]\n");
                exit(-1);
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();

        DEEP_LOG(INFO, OTHER, " SUCCESS\n");
}

void caseBigRead() {
	DEEP_LOG(INFO, OTHER, " TEST CASE - BIG READ\n");
	initFile();

	nbyte* buf = new nbyte(BUFFER_SIZE*3);
	memset((bytearray)*buf, 0xFF, buf->length);
	braFile->writeRaw(buf, 0, buf->length);
	braFile->flush();
	braFile->seek(0);

	if (braFile->length() != buf->length) {
		DEEP_LOG(ERROR, OTHER, " FAILED FILE LENGTH, EXPECTED: [%d] GOT: [%lld]\n", buf->length, braFile->length());
		exit(-1);
	}

	memset((bytearray)*buf, 0x0, buf->length);
	inttype ret = braFile->read(buf, 0, buf->length);

	if (ret != buf->length) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ LENGTH, EXPECTED: [%d] GOT: [%d]\n", buf->length, ret);
		exit(-1);
	}

	for (inttype i=0; i<buf->length; ++i) {
		if ((((bytearray)*buf)[i]) != (chartype)0xFF) {
			DEEP_LOG(ERROR, OTHER, " FAILED FILE READ, EXPECTED: [0x%1x] GOT: [0x%1x]\n", (uchartype)0xFF, (uchartype)(((bytearray)*buf)[i]));
			exit(-1);
		}
	}

	DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	file->clobber();
	delete braFile;
	delete file;
}

void caseUnevenFile() {
	DEEP_LOG(INFO, OTHER, " TEST CASE - UNEVEN FILE\n");
	initFile();

	nbyte* buf = new nbyte(BUFFER_SIZE);
	memset((bytearray)*buf, 0xFF, buf->length);
	braFile->writeRaw(buf, 0, buf->length);
	braFile->writeRaw(buf, 0, buf->length-1);
	braFile->flush();
	braFile->seek(0);

	if (braFile->length() != (BUFFER_SIZE*2-1)) {
		DEEP_LOG(ERROR, OTHER, " FAILED FILE LENGTH, EXPECTED: [%d] GOT: [%lld]\n", (BUFFER_SIZE*2-1), braFile->length());
		exit(-1);
	}

	inttype ret = 0;
	memset((bytearray)*buf, 0x0, buf->length);
	ret = braFile->read(buf, 0, buf->length);

	if (ret != buf->length) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ(1) LENGTH, EXPECTED: [%d] GOT: [%d]\n", buf->length, ret);
		exit(-1);
	}

	memset((bytearray)*buf, 0x0, buf->length);
	ret = braFile->read(buf, 0, buf->length);

	if (ret != (buf->length-1)) {
		DEEP_LOG(ERROR, OTHER, " FAILED READ(2) LENGTH, EXPECTED: [%d] GOT: [%d]\n", buf->length-1, ret);
		//exit(-1);
	}

	for (inttype i=0; i<buf->length-1; ++i) {
		if ((((bytearray)*buf)[i]) != (chartype)0xFF) {
			DEEP_LOG(ERROR, OTHER, " FAILED FILE READ, EXPECTED: [0x%1x] GOT: [0x%1x]\n", (uchartype)0xFF, (uchartype)(((bytearray)*buf)[i]));
			exit(-1);
		}
	}
	if ((((bytearray)*buf)[buf->length-1]) != (chartype)0x00) {
		DEEP_LOG(ERROR, OTHER, " FAILED FILE READ, EXPECTED: [0x%1x] GOT: [0x%1x]\n", (uchartype)0x00, (uchartype)(((bytearray)*buf)[buf->length-1]));
		exit(-1);
	}

	DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	file->clobber();
	delete braFile;
	delete file;
}

void caseCompressionLargeAlloc() {
	DEEP_LOG(INFO, OTHER, " TEST CASE - COMPRESSION LARGE ALLOC\n");
	ulongtype cacheSizeBefore = Properties::getCacheSize();
	Properties::setCacheSize(1000000000);
        startup(true, true);

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

	DEEP_LOG(INFO, OTHER, " CREATING 1000 1MB ROWS\n");
        nbyte data(1000000); /* 1 MB values */
	int x = 0;
	for (; x < 1000000; x++) {
		data[x] = (bytetype)x;
	}
	
	int rows = 1000;

	for (x = 0; x < rows; x++) {
		MAP->put(x, &data, RealTimeMap<int>::UNIQUE, tx);

		if ((x % 100) == 0) {
			tx->commit(tx->getLevel());
			Transaction::destroy(tx);

			tx = Transaction::create();
			tx->begin();
			MAP->associate(tx);
		}
	}

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, " OPTIMIZING MAP\n");
	MAP->optimize(RealTimeMap<nbyte*>::OPTIMIZE_OFFLINE_PAIRED);

	DEEP_LOG(INFO, OTHER, " RESTARTING MAP\n");
        shutdown();
        DEEP_LOG(INFO, OTHER, " SLEEPING 3 SECONDS FOR SHUTDOWN...\n");
        Thread::sleep(3000);
        startup(false, true);
	

	DEEP_LOG(INFO, OTHER, " RETRIEVING ROWS\n");

        tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

	int retKey;
	for (x = 0; x < rows; x++) {
		if (MAP->get(x, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED COMPRESSED LARGE ALLOC READ AT [%d] \n", x);
			exit(1);
		}
	}

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);

        shutdown();
	Properties::setCacheSize(cacheSizeBefore);
        DEEP_LOG(INFO, OTHER, " SUCCESS\n");
}

longtype writeData() {

	bytearray data = (bytearray) malloc(CHUNK);

	uinttype total = 0;
	char filler = 'a';

	braFile->setNofityFlushed(true);
	longtype position = braFile->getPosition() > 0 ? braFile->getPosition() : 0;

	while (total < BUFFER_SIZE * MULTIPLIER) {
		
		for (int x = 0; x < CHUNK; x++) {
			data[x] = filler;	
		}	
		
		nbyte* nbytedata = new nbyte((bytearray)data, CHUNK);

		braFile->write(nbytedata);
		delete nbytedata;

		total += CHUNK;

		filler++;

		if (braFile->flushed()) {
			DEEP_LOG(INFO, OTHER, " FLUSHED DATA\n");
			braFile->setNofityFlushed(false);
			braFile->setNofityFlushed(true);
		}
	}

	free(data);
	return position;
}

void readData(longtype position) {
	
	nbyte* data = new nbyte(CHUNK);
	uinttype total = 0;
	char filler = 'a';

	if (position != -1) {
		braFile->seek(position);	
	}

	while (total < BUFFER_SIZE * MULTIPLIER) {

		braFile->read(data);

		for (int x = 0; x < data->length; x++) {
			if (((bytearray)(*data))[x] != filler) {
				DEEP_LOG(ERROR, OTHER, " FAILED READ EXPECTED: [%c] GOT: [%c]\n", filler, (((bytearray)(*data))[x]));
				exit(-1);
			}
		}

		total += data->length;
		filler++;
	}

	delete data;
}

void initFile() {

	DEEP_LOG(INFO, OTHER, " CREATING BUFFERED RANDOM ACCESS FILE\n");
        file = new File("braFile.test");
        file->clobber();

        braFile = new BufferedRandomAccessFile(file, "rw", BUFFER_SIZE);
        braFile->setOnline(true);
}	

void startup(boolean del, boolean varCompValues) {

	DEEP_LOG(INFO, OTHER, " STARTUP MAP\n");

        // XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
        longtype options = RealTimeMap<int>::O_CLEANUP | RealTimeMap<int>::O_KEYCOMPRESS | RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY;

	if (varCompValues == true ) {
		options |= RealTimeMap<int>::O_VALUECOMPRESS;
	}

        if (del == true) {
                options |= RealTimeMap<int>::O_DELETE;
        }

	COMPARATOR = new Comparator<int>();
	KEY_BUILDER = new KeyBuilder<int>();

        MAP = new RealTimeMap<int>("./datastore", options, sizeof(int), varCompValues ? -1 : sizeof(int), COMPARATOR, KEY_BUILDER);
        MAP->mount();
        MAP->recover(false);
}

void shutdown() {

	DEEP_LOG(INFO, OTHER, " SHUTDOWN MAP\n");

	if (MAP != null ) {
		MAP->unmount(false);

		delete MAP;
		MAP = null;

		delete COMPARATOR;
		COMPARATOR = null;
		
		delete KEY_BUILDER;
		KEY_BUILDER = null;
	}
}
