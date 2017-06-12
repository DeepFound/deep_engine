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
#ifndef CXX_UTIL_QUEUESET_H_
#define CXX_UTIL_QUEUESET_H_

#include "cxx/util/Converter.h"
#include "cxx/util/Set.h"
#include "cxx/util/Logger.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename E, typename Q>
struct QueueSetUtil {
	FORCE_INLINE static void queueRemove(Q& queue, const E& e) {
		queue.remove(e);
	}
};

template<typename E, typename Q, typename S, typename L, void (*QR)(Q&, const E&) = QueueSetUtil<E,Q>::queueRemove>
class QueueSet : public Set<E> {
	private:
		Q m_queue;
		S m_set;
		L m_lock;

	public:
		FORCE_INLINE QueueSet(const Q& q, const S& s, const L& l) :
			m_queue(q),
			m_set(s),
			m_lock(l) {
		}

		FORCE_INLINE QueueSet(const QueueSet& qset) :
			m_queue(qset.m_queue),
			m_set(qset.m_set),
			m_lock() {
		}

		FORCE_INLINE virtual ~QueueSet() {
		}

		FORCE_INLINE virtual E get(inttype i) const {
			E ret = NULL_VALUE;
			lock();
			{
				ret = m_queue.get(i);
			}
			unlock();
			return ret;
		}

		FORCE_INLINE virtual void set(inttype i, const E e) {
			lock();
			{
				if (m_set.contains(e) == true) {
					unlock();
					return;
				}

				if (i >= m_queue.capacity()) {
					m_queue.resize((i+1)*2);
				}
				if (i >= m_queue.size()) {
					const inttype oldsize = m_queue.size();
					m_queue.size(i+1);
					// XXX: clear out unused slots in sparse queue case
					for (inttype j=oldsize; j<i; ++j) {
						m_queue.set(j, (E)Converter<E>::NULL_VALUE);
					}
				}

				m_set.add(e);
				m_queue.set(i, e);
			}
			unlock();
		}

		FORCE_INLINE virtual int size() const {
			inttype ret = 0;
			lock();
			{
				#ifdef DEEP_DEBUG
				if (m_queue.size() < m_set.size()) { // TODO: add better check for sparse queue implementations
					DEEP_LOG(ERROR, OTHER, "QueueSet size mismatch: %d != %d\n", m_queue.size(), m_set.size());
					throw RuntimeException("QueueSet size mismatch");
				}
				#endif
				ret = m_set.size();
			}
			unlock();
			return ret;
		}

		FORCE_INLINE virtual boolean isEmpty() const {
			boolean ret = false;
			lock();
			{
				#ifdef DEEP_DEBUG
				if (m_queue.isEmpty() != m_set.isEmpty()) {
					DEEP_LOG(ERROR, OTHER, "QueueSet size mismatch: %d != %d\n", m_queue.size(), m_set.size());
					throw RuntimeException("QueueSet size mismatch");
				}
				#endif
				ret = m_set.isEmpty();
			}
			unlock();
			return ret;
		}

		FORCE_INLINE virtual boolean contains(const E e) const {
			return contains(e, true);
		}
		
		FORCE_INLINE boolean contains(const E e, const boolean needsLock) const {
			boolean ret = false;
			if (needsLock == true) lock();
			{
				ret = m_set.contains(e);
			}
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE virtual Iterator<E>* iterator() {
			return iterator(true /* useQueue */, true);
		}

		FORCE_INLINE Iterator<E>* iterator(boolean useQueue, const boolean needsLock, boolean forward = true) {
			Iterator<E>* ret = null;
			if (needsLock == true) lock();
			{
				if (forward == true) {
					ret = new QueueSetIterator<true>(this, useQueue, needsLock);
				} else {
					ret = new QueueSetIterator<false>(this, useQueue, needsLock);
				}
			}
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE virtual boolean add(E e) {
			return add(e, true);
		}

		FORCE_INLINE boolean add(E e, const boolean needsLock) {
			if (needsLock == true) lock();
			{
				if (m_set.contains(e) == true) {
					if (needsLock == true) unlock();
					return false;
				}

				m_set.add(e);
				m_queue.add(e);
			}
			if (needsLock == true) unlock();

			return true;
		}

		FORCE_INLINE virtual boolean remove(const E e) {
			return remove(e, true);
		}

		FORCE_INLINE boolean remove(const E e, const boolean needsLock) {
			if (needsLock == true) lock();
			{
				if (m_set.contains(e) == false) {
					if (needsLock == true) unlock();
					return false;
				}
				m_set.remove(e);
				QR(m_queue, e);
			}
			if (needsLock == true) unlock();
			return true;
		}

		FORCE_INLINE virtual void clear() {
			return clear(true);
		}

		FORCE_INLINE void clear(const boolean needsLock) {
			if (needsLock == true) lock();
			{
				m_set.clear();
				m_queue.clear();
			}
			if (needsLock == true) unlock();
		}

		FORCE_INLINE void lock() const {
			const_cast<L*>(&m_lock)->lock();
		}

		FORCE_INLINE void unlock() const {
			const_cast<L*>(&m_lock)->unlock();
		}

		template<boolean forward>
		class QueueSetIterator: public Iterator<E> {
			private:
				QueueSet<E,Q,S,L,QR>* m_queueSet;
				Iterator<E>* m_iter;
				const boolean m_useQueue;
				const boolean m_needsLock;
				E m_last;
				boolean m_canRemove;

			public:
				FORCE_INLINE QueueSetIterator(QueueSet<E,Q,S,L,QR>* queueSet, boolean useQueue = true, boolean needsLock = true) :
					m_queueSet(queueSet),
					m_iter((useQueue == true) ? queueSet->m_queue.iterator() : queueSet->m_set.iterator()),
					m_useQueue(useQueue),
					m_needsLock(needsLock),
					m_last(QueueSet<E,Q,S,L,QR>::NULL_VALUE),
					m_canRemove(false) {
				}

				FORCE_INLINE virtual ~QueueSetIterator() {
					Converter<Iterator<E>*>::destroy(m_iter);
				}

				FORCE_INLINE virtual boolean hasNext() {
					boolean ret = false;
					if (m_needsLock == true) m_queueSet->lock();
					{
						ret = m_iter->hasNext();
					}
					if (m_needsLock == true) m_queueSet->unlock();
					return ret;
				}

				FORCE_INLINE virtual const E next() {
					if (m_needsLock == true) m_queueSet->lock();
					{
						m_last = m_iter->next();
						m_canRemove = true;
					}
					if (m_needsLock == true) m_queueSet->unlock();
					return m_last;
				}

				FORCE_INLINE virtual void remove() {
					if (m_needsLock == true) m_queueSet->lock();
					{
						if (m_canRemove == false) {
							if (m_needsLock == true) m_queueSet->unlock();

							throw RuntimeException("Unable to remove element");
						}

						m_iter->remove();
						if (m_useQueue == true) {
							m_queueSet->m_set.remove(m_last);
						} else {
							QR(m_queueSet->m_queue, m_last);
						}
						m_canRemove = false;
					}
					if (m_needsLock == true) m_queueSet->unlock();
				}
		};

	protected:
		static const E NULL_VALUE;
};

template<typename E, typename Q, typename S, typename L, void (*QR)(Q&, const E&)>
const E QueueSet<E,Q,S,L,QR>::NULL_VALUE = (const E)Converter<E>::NULL_VALUE;

} } // namespace

#endif /*CXX_UTIL_QUEUESET_H_*/
