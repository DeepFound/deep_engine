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
#ifndef CXX_LANG_THREADLOCAL_H_
#define CXX_LANG_THREADLOCAL_H_

#include <pthread.h>

#include "cxx/lang/Object.h"

namespace cxx { namespace lang {

template<typename T>
class ThreadLocal : public Object {

	public:
		class Callback {

			public:
				virtual void exit(T value) = 0;
		};

	private:
		class Context {

			private:
				T m_value;
				Callback* m_callback;
				boolean m_deleteValue;

			public:
				FORCE_INLINE Context(T value, Callback* callback, boolean delval):
					m_value(value),
					m_callback(callback),
					m_deleteValue(delval) {
				}

				FORCE_INLINE ~Context(void) {
					setValue(null);
				}

				FORCE_INLINE void setValue(T value) {
					if (m_deleteValue == true) {
						delete m_value;
					}

					m_value = value;
				}

				FORCE_INLINE T getValue() {
					return m_value;
				}

				FORCE_INLINE void setDeleteValue(boolean delval) {
					m_deleteValue = delval;
				}

				FORCE_INLINE boolean getDeleteValue(void) {
					return m_deleteValue;
				}

				FORCE_INLINE void setCallback(Callback* callback) {
					m_callback = callback;
				}

				FORCE_INLINE Callback* getCallback(void) {
					return m_callback;
				}
		};

	private:
		Callback* m_callback;
		boolean m_deleteValue;

		pthread_key_t m_key;

		FORCE_INLINE static void destructor(void* value) {
			Context* context = (Context*) value;
			if (context != null) {
				Callback* callback = context->getCallback();
				if (callback != null) {
					callback->exit(context->getValue());
				}

				delete context;
			}
		}

	public:
		FORCE_INLINE ThreadLocal(Callback* callback = null, boolean delval = true):
			m_callback(callback),
			m_deleteValue(delval) {

			pthread_key_create(&m_key, destructor);
		}

		virtual ~ThreadLocal(void) {
			pthread_key_delete(m_key);
		}

		FORCE_INLINE void set(T value) {
			Context* context;
			context = (Context*) pthread_getspecific(m_key);

			if (context != null) {
				context->setValue(value);
				context->setCallback(m_callback);
				context->setDeleteValue(m_deleteValue);

			} else {
				context = new Context(value, m_callback, m_deleteValue);
			}

			pthread_setspecific(m_key, context);
		}

		FORCE_INLINE T get(void) const {
			Context* context;

			context = (Context*) pthread_getspecific(m_key);

			return (context != null) ? context->getValue() : null;
		}

		FORCE_INLINE void remove(void) {
			Context* context;

			context = (Context*) pthread_getspecific(m_key);
			pthread_setspecific(m_key, null);

			delete context;
		}
};

} } // namespace

#endif /*CXX_LANG_THREADLOCAL_H_*/
