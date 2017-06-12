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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_CXX_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_CXX_

#include "com/deepis/db/store/relative/core/RealTimeVersion.h"

using namespace com::deepis::db::store::relative::core;

/* IRT HEAD BODY FORMAT
	1 : segment flags
	2 : lrt file index
	4 : lrt file length
	4 : virtual size
	? : cardinality
	1 : vrt file range
	2 : vrt file index
	1 : irt file range
	2 : irt file index
	4 : irt reference
	2 : physical size
	4 : pre segment location
	4 : post segment location
	---------------------
	31 - total offset size
*/
template<int V, typename K>
const inttype RealTimeProtocol_v1_0_0_0<V,K>::IRT_HEAD_FORMAT_FIX = 31;

/* FIX - IRT INFO BODY FORMAT
	1 : index flags
	2 : lrt file index || total map entries (lo)
	4 : value position || total map entries (hi)
	? : size of key
	---------------------
	7 - total offset size
*/
template<int V, typename K>
const inttype RealTimeProtocol_v1_0_0_0<V,K>::IRT_BODY_FORMAT_FIX = 7;

/* VAR - IRT INFO BODY FORMAT
	1 : index flags
	2 : lrt file index
	4 : value position
	4 : value size
	? : size of key
	---------------------
	11 - total offset size
*/
template<int V, typename K>
const inttype RealTimeProtocol_v1_0_0_0<V,K>::IRT_BODY_FORMAT_VAR = 11;

/* FIX - LRT INFO BODY FORMAT
	1 : log flags
	? : size of key
	---------------------
	1 - total offset size
*/
template<int V, typename K>
const inttype RealTimeProtocol_v1_0_0_0<V,K>::LRT_BODY_FORMAT_FIX = 1;

/* VAR - LRT INFO BODY FORMAT
	1 : log flags
	4 : value position
	4 : value size
	? : size of key
	---------------------
	9 - total offset size
*/
template<int V, typename K>
const inttype RealTimeProtocol_v1_0_0_0<V,K>::LRT_BODY_FORMAT_VAR = 9;

template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_OPENING = 0x01;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_CLOSING = 0x02;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_PENDING = 0x04;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_ROLLING = 0x08;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_DESTROY = 0x10;
// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_MARKING = 0x20;

template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_CONTENT = 0;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_RESERVE = 1;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_DELETED = 2;
template<int V, typename K>
const bytetype RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_CLOSURE = 3;

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_CXX_*/
