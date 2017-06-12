#include  <signal.h>
#include  <sys/wait.h>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"
#include "cxx/util/HashMap.cxx"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_CXX_
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"


using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;
using namespace com::deepis::db::store::relative::util;

static int COUNT = 4;
static int DEFAULT_FILE_BUFFER = 1024;

class TestWriter {
	private:
		MeasuredRandomAccessFile* m_mraFile;

		inttype     m_i;
		uinttype    m_dataSize;
		boolean     m_test;

	public:
		TestWriter(inttype i) :
			m_i(i),
			m_dataSize((i%3) ? (500 + i) : (2000 + i)), // don't cause flush on every third run (i.e. dataSize < 1024)
			m_test((i%2) ? true : false) {                  // cause size adjusting logic every other run

			m_mraFile = new MeasuredRandomAccessFile("mraFile.test", "rw", DEFAULT_FILE_BUFFER);
			m_mraFile->seek(m_mraFile->length());
		}

		~TestWriter() {
			delete m_mraFile;
		}

		void write() {

			DEEP_LOG(INFO, OTHER, "-->BEGIN WRITE SEGMENT %d<--\n", m_i);

			ushorttype sizeBefore = m_dataSize - (m_test?1:0);
			ushorttype sizeAfter = m_dataSize;

			m_mraFile->setNofityFlushed(true);

			DEEP_LOG(INFO, OTHER, "position: %lld, writeInt: %d\n", m_mraFile->getFilePointer(), 1);
			m_mraFile->writeInt(1); // lrt size

			DEEP_LOG(INFO, OTHER, "position: %lld, writeByte: %d\n", m_mraFile->getFilePointer(), 1);
			m_mraFile->writeByte(1); // state flags

			const inttype cursorBefore = m_mraFile->getCursor();
			const longtype positionBefore = m_mraFile->getPosition();

			DEEP_LOG(INFO, OTHER, "position: %lld, writeByte: %d\n", m_mraFile->getFilePointer(), 1);
			m_mraFile->writeByte(1); //range

			DEEP_LOG(INFO, OTHER, "position: %lld, writeShort: %d\n", m_mraFile->getFilePointer(), 1);
			m_mraFile->writeShort(1); // index

			DEEP_LOG(INFO, OTHER, "position: %lld, writeInt: %d\n", m_mraFile->getFilePointer(), 1);
			m_mraFile->writeInt(1); // file position

			DEEP_LOG(INFO, OTHER, "position: %lld, writeShort (sizebefore): %u\n", m_mraFile->getFilePointer(), sizeBefore);
 			m_mraFile->writeShort(sizeBefore);

			// XXX: like stream mode (i.e. map::add), position excludes the above segment header
			//const uinttype position = m_mraFile->BufferedRandomAccessFile::getFilePointer();

			nbyte buffer(m_dataSize);

			DEEP_LOG(INFO, OTHER, "position: %lld, write buffer size: %u\n", m_mraFile->getFilePointer(), m_dataSize);

			m_mraFile->write(&buffer);

			DEEP_LOG(INFO, OTHER, "Flushed: %d\n", m_mraFile->flushed());

			if (sizeBefore != sizeAfter) {
				const inttype cursorAfter = m_mraFile->getCursor();
				const longtype positionAfter = m_mraFile->getPosition();

				if (m_mraFile->flushed() == false) {
					m_mraFile->setCursor(cursorBefore);
					m_mraFile->setPosition(positionBefore);

				} else {
					m_mraFile->flush();
					m_mraFile->seek(positionBefore);
				}

				DEEP_LOG(INFO, OTHER, "position: %lld, adjusting writeByte: %u\n", m_mraFile->getFilePointer(), 2);
				m_mraFile->writeByte(2 /* brt file range */);

				DEEP_LOG(INFO, OTHER, "position: %lld, adjusting writeShort: %u\n", m_mraFile->getFilePointer(), 2);
				m_mraFile->writeShort(2 /* brt file index */);

				DEEP_LOG(INFO, OTHER, "position: %lld, adjusting writeInt: %u\n", m_mraFile->getFilePointer(), 2);
				m_mraFile->writeInt(2 /* brt reference */);

				DEEP_LOG(INFO, OTHER, "position: %lld, adjusting writeShort (sizebefore): %u\n", m_mraFile->getFilePointer(), sizeAfter);
				m_mraFile->writeShort(sizeAfter);

				if (m_mraFile->flushed() == false) {
					m_mraFile->setCursor(cursorAfter);
					m_mraFile->setPosition(positionAfter);

				} else {
					m_mraFile->flush();
					m_mraFile->seek(m_mraFile->length());
				}
			}

			longtype afterPosition = m_mraFile->getFilePointer();
			DEEP_LOG(INFO, OTHER, "position: %lld, writeShort (sizeafter): %u\n", afterPosition, sizeAfter);

			m_mraFile->writeShort(sizeAfter /* post segment size */);
			m_mraFile->flush();

			if (ftell(m_mraFile->getHandle()) != afterPosition + 2) {
				DEEP_LOG(ERROR, OTHER, "WROTE TO THE END OF FILE INSTEAD OF SEEKING, Expected length: %lld, Actual length: %ld\n", afterPosition + 2, ftell(m_mraFile->getHandle()));
				exit(1);
			}

			m_mraFile->setNofityFlushed(false);

			DEEP_LOG(INFO, OTHER, "-->END WRITE SEGMENT %d<--\n\n", m_i);
		}
};

class TestReader {
	private:
		#if 1
		BufferedRandomAccessFile* m_braFile;
		#else
		RandomAccessFile* m_braFile;
		#endif
		inttype m_segments;

	public:
		TestReader(inttype segments) :
			m_segments(segments) {

			#if 1
			m_braFile = new BufferedRandomAccessFile("mraFile.test", "r", DEFAULT_FILE_BUFFER);
			#else
			m_braFile = new RandomAccessFile("mraFile.test", "r");
			#endif
		}

		~TestReader() {
			delete m_braFile;
		}

		void read() {
			m_braFile->seek(0);

			for (int i=0; i<m_segments; i++) {
				DEEP_LOG(INFO, OTHER, "-->BEGIN READ SEGMENT %d<--\n", i);

				longtype fp = m_braFile->getFilePointer();
				inttype d = m_braFile->readInt();
				DEEP_LOG(INFO, OTHER, "position: %lld, readInt: %d\n", fp, d);

				fp = m_braFile->getFilePointer();
				d = m_braFile->readByte();
				DEEP_LOG(INFO, OTHER, "position: %lld, readByte: %d\n", fp, d);

				fp = m_braFile->getFilePointer();
				d = m_braFile->readByte();
				DEEP_LOG(INFO, OTHER, "position: %lld, readByte: %d\n", fp, d);

				fp = m_braFile->getFilePointer();
				d = m_braFile->readShort();
				DEEP_LOG(INFO, OTHER, "position: %lld, readShort: %d\n", fp, d);

				fp = m_braFile->getFilePointer();
				d = m_braFile->readInt();
				DEEP_LOG(INFO, OTHER, "position: %lld, readInt: %d\n", fp, d);

				fp = m_braFile->getFilePointer();
				ushorttype sizebefore = m_braFile->readShort();
				DEEP_LOG(INFO, OTHER, "position: %lld, readShort (sizebefore): %d\n", fp, sizebefore);

				fp = m_braFile->getFilePointer();
				DEEP_LOG(INFO, OTHER, "position: %lld, skipping buffer bytes length: %d\n", fp, sizebefore);
				m_braFile->skipBytes(sizebefore);

				fp = m_braFile->getFilePointer();
				ushorttype sizeafter = m_braFile->readShort();
				DEEP_LOG(INFO, OTHER, "position: %lld, readShort (sizeafter): %d\n", fp, sizeafter);

				if (sizeafter != sizebefore) {
					DEEP_LOG(ERROR, OTHER, "FAILED! sizebefore: %u does not match after sizeafter: %u\n", sizebefore, sizeafter);
					exit(1);
				}

				DEEP_LOG(INFO, OTHER, "-->END READ SEGMENT %d<--\n\n", i);
			}
		}
};

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);

	File file("mraFile.test");
	file.clobber();

	DEEP_LOG(INFO, OTHER, "\n\nDEFAULT_FILE_BUFFER: %d\n\n", DEFAULT_FILE_BUFFER);

	for (int i=0; i<COUNT; i++) {
		TestWriter* testWriter = new TestWriter(i);

		testWriter->write();

		delete testWriter;
	}

	TestReader* testReader = new TestReader(COUNT);

	testReader->read();

	delete testReader;

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}
