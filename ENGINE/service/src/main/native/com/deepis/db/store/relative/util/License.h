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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LICENSE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LICENSE_H_ 

#include "cxx/io/FileUtil.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class License {	

	private:
		class DeepLicense {
			public:
				enum LicenseType {
					UNKNOWN    = -1,
					DEVELOPER  = 0,
					STANDARD   = 1,
					ENTERPRISE = 2,
					DEFAULT    = 3
				};

			private:
				boolean m_valid;

				 // TODO: encode/encrypt this to be a bit less obvious
				longtype m_expirationDate;

				LicenseType m_type;

				String m_version;
				String m_activationKey;
				String m_billingCode; // 15 character code from SF

				// 02.01.00-01.0000-000000000000000-1427830474
				// major.minor.build-type.usage-billingID-expiration
				void decode() {
					if (m_activationKey.length() == 33) {
						m_valid = true;

						// TODO: 
						m_version = m_activationKey.substring(0, 6);

						String type = m_activationKey.substring(6, 8);
						if (type.startsWith("00") == true) {
							m_type = DEVELOPER;

						} else if (type.startsWith("01") == true) {
							m_type = STANDARD;

						} else if (type.startsWith("02") == true) {
							m_type = ENTERPRISE;

						} else {
							m_type = DEFAULT;
							m_valid = false;
						}

						if (m_valid == true) {
							// TODO: 
							m_billingCode = m_activationKey.substring(12, 23);

							String expiration = m_activationKey.substring(23);
							m_expirationDate = atoll(expiration.data());
						}
					}
				}

			public:
				DeepLicense(const char* key) :
					m_valid(false),
					m_expirationDate(0),
					m_type(UNKNOWN),
					m_version(""),
					m_activationKey(key),
					m_billingCode("") {
			
					decode();
				}

				boolean isValid() const {
					return m_valid;
				}

				LicenseType getType() const {
					return m_type;
				}

				boolean isExpired() const {
					if (isValid() == true) {
						longtype cur_date = System::currentTimeMillis() / 1000;

						return (cur_date > m_expirationDate);

					} else {
						return true;
					}
				}

				inttype getExpirationDays() const {
					if (isValid() == true) {
						return (m_expirationDate - System::currentTimeMillis()/1000)/60/60/24;

					} else {
						return 0;
					}
				}

				const char* getKey() const {
					return m_activationKey.data();
				}

				boolean isDefault() const {
					return m_activationKey.startsWith(CT_DATASTORE_ACTIVATION_KEY_DEFAULT_STR);
				}
		};

	public:
		static inttype validateActivationKey(const char* key) {
			inttype status = CT_DATASTORE_ACTIVATION_KEY_VALID;

			DeepLicense deepLicense(key);

			if(deepLicense.isValid() == true) {
				if(deepLicense.isExpired() == true) {
					status = CT_DATASTORE_ACTIVATION_KEY_EXPIRED;
				}

			} else if (deepLicense.isDefault() == true) {
					status = CT_DATASTORE_ACTIVATION_KEY_DEFAULT;

			} else {
				status = CT_DATASTORE_ACTIVATION_KEY_INVALID;
			}

			return status;
		}

		/* XXX: Decode a given license and log license info to file */
		static void logActivationStatus(const char* key) {
			DeepLicense deepLicense(key);

			switch(deepLicense.getType()) {
				case DeepLicense::DEVELOPER:
					DEEP_LOG(INFO,LCNSE,"License type: Deep Engine Developer Edition – For development and test use only, "
										"not for commercial use, License expiration: None\n");
					break;
				case DeepLicense::STANDARD:
					DEEP_LOG(INFO,LCNSE,"License type: Deep Engine Standard Edition – Licensed for commercial use by "
										"businesses with less than $1M in revenue and for educational purposes, License "
										"expiration: None.\n");
					break;
				case DeepLicense::ENTERPRISE:
					DEEP_LOG(INFO,LCNSE,"License type: Deep Engine Enterprise Edition – Licensed for unrestricted "
										"commercial use up to the authorized capacity. Please check the license terms "
										"for License ID: %s for details, License expiration in %d days.\n",
										deepLicense.getKey(), deepLicense.getExpirationDays());
					break;
				case DeepLicense::UNKNOWN:
				case DeepLicense::DEFAULT:
				default:
					DEEP_LOG(INFO,LCNSE,"License type: Deep Engine Standard Edition (default) – Licensed for commercial "
										"use by businesses with less than $1M in revenue and for educational purposes, "
										"License expiration: None.\n");
					break;
			}
		}

		/* XXX:
		Return machine data - Options:
		Return a volume id
		Return the lowest value mac
		Return a hash of all macs
		*/
		static String getMachineData(void) {
			String mac = "fnord";
			#if 0
			String dname = "/sys/class/net/";
			ArrayList<String*>* list = FileUtil::list(dname);

			for (inttype i = 0; i < list->size(); i++) {
				String* item = list->get(i);

				if (*item == "." || *item == "..") {
					continue;
				}

				String fname = dname;
				fname.append(*item);
				fname.append("/address");

				File tmpfile = File(fname);
				if(!tmpfile.exists()) 
					continue;

				RandomAccessFile f = RandomAccessFile( fname, "r", true);
				f.seek(0);
				boolean eof = true;

				nbyte tmp(65);
				tmp.zero();
				f.readFully(&tmp, 0, 64, &eof);

				String tmpmac = (char*) tmp;
	
				f.close();
				if(mac.compare(tmpmac) != 0) {
					mac = tmpmac;
				}
			}
			delete list;
			#endif
			
			return mac;
		}
	
		/* XXX:
		Read in some unique machine data
		encode it a little bit
		*/
		static inttype generateSystemUid(void) {
			inttype hash = 0;
			#if 0
			String machine_data = getMachineData();
			for(String::const_iterator iter=machine_data.begin(); iter!=machine_data.end(); ++iter) {
				hash = hash << 1 ^ (*iter - 3);
			}
			#endif
			return hash;
		}
};

} } } } } } // namespace

#endif // COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LICENSE_H_ 
