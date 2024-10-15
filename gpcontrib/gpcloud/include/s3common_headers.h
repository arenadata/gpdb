#ifndef __S3_COMMON_HEADERS_H__
#define __S3_COMMON_HEADERS_H__

#include <curl/curl.h>
#include <fcntl.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <zlib.h>
#include <algorithm>
#include <csignal>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using std::map;
using std::string;
using std::stringstream;
using std::vector;

#define DO_PRAGMA(x) _Pragma (#x)
#if defined(__clang__)
	#define SUPPRESS_COMPILER_WARNING(expr, warning) do { \
		DO_PRAGMA(clang diagnostic push) \
		DO_PRAGMA(clang diagnostic ignored warning) \
		expr; \
		DO_PRAGMA(clang diagnostic pop) \
	} while(0)
#elif defined(__GNUC__)
	#define SUPPRESS_COMPILER_WARNING(expr, warning) do { \
		DO_PRAGMA(GCC diagnostic push) \
		DO_PRAGMA(GCC diagnostic ignored warning) \
		expr; \
		DO_PRAGMA(GCC diagnostic pop) \
	} while(0)
#else
	/*
	 * We can't support all compilers because the semantics of pragma
	 * differs across them as well as error codes. It's easy to add
	 * clang support because it supports GNU-style flags, but the others
	 * may not. For such cases, we need a fallback option
	*/
	expr;
#endif

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "http_parser.h"
#include "ini.h"

#define DATE_STR_LEN 9
#define TIME_STAMP_STR_LEN 17

class S3Params;

extern string s3extErrorMessage;

class UniqueLock {
   public:
    UniqueLock(pthread_mutex_t *m) : mutex(m) {
        pthread_mutex_lock(this->mutex);
    }
    ~UniqueLock() {
        pthread_mutex_unlock(this->mutex);
    }

   private:
    pthread_mutex_t *mutex;
};

struct S3Credential {
    bool operator==(const S3Credential &other) const {
        return this->accessID == other.accessID && this->secret == other.secret &&
               this->token == other.token;
    }

    string accessID;
    string secret;
    string token;
};

S3Params InitConfig(const string &urlWithOptions);

void MaskThreadSignals();
#endif
