/*
 * simspider-redis
 * author	: calvin
 * email	: calvinwilliams.c@gmail.com
 *
 * Licensed under the LGPL v2.1, see the file LICENSE in base directory.
 */

#ifndef _H_LIBSIMSPIDER_REDIS_
#define _H_LIBSIMSPIDER_REDIS_

#include "libsimspider.h"
#include "hiredis.h"

#ifdef __cplusplus
extern "C" {
#endif

extern char    *__SIMSPIDER_REDIS_VERSION ;

#define SIMSPIDER_REDIS_REQUESTQUEUE		"SIMSPIDER_REDIS_REQUESTQUEUE"
#define SIMSPIDER_REDIS_DONEQUEUE		"SIMSPIDER_REDIS_DONEQUEUE"

_WINDLL_FUNC int BindSimspiderRedisQueueHandler( struct SimSpiderEnv *penv , char *redis_ip , long redis_port , int select_index );
_WINDLL_FUNC void UnbindSimspiderRedisQueueHandler( struct SimSpiderEnv *penv );

#ifdef __cplusplus
}
#endif

#endif

