/*
 * simspider-redis - Web Multi-Processes Spider With Redis
 * author	: calvin
 * email	: calvinwilliams.c@gmail.com
 *
 * Licensed under the LGPL v2.1, see the file LICENSE in base directory.
 */

#include "libsimspider-redis.h"
#include <sys/types.h>
#include <sys/wait.h>

long		g_process_count ;
#define MAX_PROCESS_COUNT	100

static int SetClusterProcessStatus( struct SimSpiderEnv *penv , int index , int finished_count )
{
	redisContext	*conn = NULL ;
	redisReply	*reply = NULL ;
	char		command[ 1024 + 1 ] ;
	
	conn = (redisContext*)GetDoneQueueHandler(penv) ;
	
	memset( command , 0x00 , sizeof(command) );
	snprintf( command , sizeof(command) , "SET CLUSTER_PROCESS_STATUS:%d %d" , index , finished_count );
	reply = redisCommand( conn , command ) ;
	if( reply && reply->type == REDIS_REPLY_STATUS && strcasecmp( reply->str , "OK" ) == 0 )
	{
		DebugLog( __FILE__ , __LINE__ , "redisCommand[%s] ok" , command );
		freeReplyObject( reply );
	}
	else
	{
		ErrorLog( __FILE__ , __LINE__ , "redisCommand[%s] failed[%s]" , command , conn->errstr , reply?reply->str:"" );
		freeReplyObject( reply );
		return -1;
	}
	
	return 0;
}

static int GetClusterProcessStatus( struct SimSpiderEnv *penv , int index )
{
	redisContext	*conn = NULL ;
	redisReply	*reply = NULL ;
	char		command[ 1024 + 1 ] ;
	int		finished_count ;
	
	conn = (redisContext*)GetDoneQueueHandler(penv) ;
	
	memset( command , 0x00 , sizeof(command) );
	snprintf( command , sizeof(command) , "GET CLUSTER_PROCESS_STATUS:%d" , index );
	reply = redisCommand( conn , command ) ;
	if( reply && reply->type == REDIS_REPLY_STRING )
	{
		finished_count = atoi(reply->str) ;
		DebugLog( __FILE__ , __LINE__ , "redisCommand[%s] ok , [%d]" , command , finished_count );
		freeReplyObject( reply );
	}
	else
	{
		ErrorLog( __FILE__ , __LINE__ , "redisCommand[%s] failed[%s]" , command , conn->errstr , reply?reply->str:"" );
		freeReplyObject( reply );
		return -1;
	}
	
	return finished_count;
}

static int GetRequestQueueRemainCount( struct SimSpiderEnv *penv )
{
	redisContext	*conn = NULL ;
	redisReply	*reply = NULL ;
	char		command[ 1024 + 1 ] ;
	int		request_remain_count ;
	
	conn = (redisContext*)GetDoneQueueHandler(penv) ;
	
	memset( command , 0x00 , sizeof(command) );
	snprintf( command , sizeof(command) , "LLEN "SIMSPIDER_REDIS_REQUESTQUEUE );
	reply = redisCommand( conn , command ) ;
	if( reply && reply->type == REDIS_REPLY_INTEGER )
	{
		request_remain_count = (int)(reply->integer) ;
		DebugLog( __FILE__ , __LINE__ , "redisCommand[%s] ok , [%d]" , command , request_remain_count );
		freeReplyObject( reply );
	}
	else
	{
		ErrorLog( __FILE__ , __LINE__ , "redisCommand[%s] failed[%s]" , command , conn->errstr , reply?reply->str:"" );
		freeReplyObject( reply );
		return -1;
	}
	
	return request_remain_count;
}

funcRequestHeaderProc RequestHeaderProc ;
int RequestHeaderProc( struct DoneQueueUnit *pdqu )
{
	struct curl_slist	*curl_header_list = NULL ;
	
	int			*p_index = NULL ;
	
	int			nret = 0 ;
	
	curl_header_list = GetCurlHeadListPtr( pdqu ) ;
	curl_header_list = curl_slist_append( curl_header_list , "User-Agent: Mozilla/5.0(Windows NT 6.1; WOW64; rv:34.0 ) Gecko/20100101 Firefox/34.0" ) ;
	if( curl_header_list == NULL )
	{
		ErrorLog( __FILE__ , __LINE__ , "curl_slist_append failed" );
		return SIMSPIDER_ERROR_FUNCPROC_INTERRUPT;
	}
	FreeCurlHeadList1Later( pdqu , curl_header_list );
	
	p_index = GetSimSpiderPublicData( GetSimSpiderEnv(pdqu) ) ;
	nret = SetClusterProcessStatus( GetSimSpiderEnv(pdqu) , (*p_index) , 1 );
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "SetClusterProcessStatus failed" , nret );
		return -1;
	}
	else
	{
		DebugLog( __FILE__ , __LINE__ , "SetClusterProcessStatus ok" );
	}
	
	return 0;
}

funcResponseBodyProc ResponseBodyProc ;
int ResponseBodyProc( struct DoneQueueUnit *pdqu )
{
	struct SimSpiderBuf	*buf = NULL ;
	
	buf = GetDoneQueueUnitBodyBuffer(pdqu) ;
	DebugLog( __FILE__ , __LINE__ , "[%s] HTTP BODY [%.*s]" , GetDoneQueueUnitUrl(pdqu) , (int)(buf->len) , buf->base );
	
	return 0;
}

funcFinishTaskProc FinishTaskProc ;
int FinishTaskProc( struct DoneQueueUnit *pdqu )
{
	printf( ">>> [%3d] [%2ld] [%2ld] [%s] [%s]\n" , GetDoneQueueUnitStatus(pdqu) , GetDoneQueueUnitRecursiveDepth(pdqu) , GetDoneQueueUnitRetryCount(pdqu)
		 , GetDoneQueueUnitRefererUrl(pdqu) , GetDoneQueueUnitUrl(pdqu) );
	
	return 0;
}

static int simspider_redis( int index_myself , char *ip , long port , char *url , long max_concurrent_count )
{
	struct SimSpiderEnv	*penv = NULL ;
	
	int			index ;
	int			finished_count ;
	int			request_queue_remain_count ;
	
	int			nret = 0 ;
	
	nret = InitSimSpiderEnv( & penv , "simspider-cluster.log" ) ;
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "InitSimSpiderEnv failed[%d]" , nret );
		return -1;
	}
	
	nret = BindSimspiderRedisQueueHandler( penv , ip , port , 0 ) ;
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "BindSimspiderRedisEnv failed[%d]" , nret );
		return -1;
	}
	
	ResizeRequestQueue( penv , 10*1024*1024 );
	SetMaxConcurrentCount( penv , max_concurrent_count );
	SetMaxRetryCount( penv , 10 );
	
	SetSimSpiderPublicData( penv , & index_myself );
	SetRequestHeaderProc( penv , & RequestHeaderProc );
	SetResponseBodyProc( penv , & ResponseBodyProc );
	SetFinishTaskProc( penv , & FinishTaskProc );
	
	while(1)
	{
		nret = SimSpiderGo( penv , NULL , NULL ) ;
		if( nret )
		{
			ErrorLog( __FILE__ , __LINE__ , "SimSpiderGo failed[%d]" , nret );
			break;
		}
		else
		{
			InfoLog( __FILE__ , __LINE__ , "SimSpiderGo ok" );
		}
		
		nret = SetClusterProcessStatus( penv , index_myself , GetCurlFinishedCount(penv) ) ;
		if( nret )
		{
			ErrorLog( __FILE__ , __LINE__ , "SetClusterProcessStatus failed" , nret );
			return -1;
		}
		else
		{
			DebugLog( __FILE__ , __LINE__ , "SetClusterProcessStatus ok" );
		}
		
		sleep(1);
		
		request_queue_remain_count = GetRequestQueueRemainCount( penv ) ;
		if( request_queue_remain_count < 0 )
		{
			ErrorLog( __FILE__ , __LINE__ , "GetRequestQueueRemainCount failed" , nret );
			return -1;
		}
		else
		{
			DebugLog( __FILE__ , __LINE__ , "GetRequestQueueRemainCount ok , request_queue_remain_count[%d]" , request_queue_remain_count );
		}
		
		for( index = 0 ; index < g_process_count ; index++ )
		{
			finished_count = GetClusterProcessStatus( penv , index ) ;
			if( finished_count < 0 )
			{
				ErrorLog( __FILE__ , __LINE__ , "GetClusterProcessStatus failed" , nret );
				return -1;
			}
			else
			{
				DebugLog( __FILE__ , __LINE__ , "GetClusterProcessStatus ok" );
			}
			
			if( finished_count > 0 )
				break;
		}
		if( index >= g_process_count && request_queue_remain_count == 0 )
		{
			InfoLog( __FILE__ , __LINE__ , "All is done" );
			break;
		}
	}
	
	UnbindSimspiderRedisQueueHandler( penv );
	
	CleanSimSpiderEnv( & penv );
	
	return nret;
}

static int simspider_cluster( char *ip , long port , char *url , long process_count , long max_concurrent_count )
{
	struct SimSpiderEnv	*penv = NULL ;
	
	redisContext		*conn = NULL ;
	redisReply		*reply = NULL ;
	char			command[ 1024 + 1 ] ;
	
	pid_t			pids[ MAX_PROCESS_COUNT ] ;
	int			status ;
	int			index ;
	
	int			nret = 0 ;
	
	if( process_count > MAX_PROCESS_COUNT )
	{
		ErrorLog( __FILE__ , __LINE__ , "Too many processes" );
		return -1;
	}
	
	g_process_count = process_count ;
	
	nret = InitSimSpiderEnv( & penv , "simspider-cluster.log" ) ;
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "InitSimSpiderEnv failed[%d]" , nret );
		return -1;
	}
	
	nret = BindSimspiderRedisQueueHandler( penv , ip , port , 0 ) ;
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "BindSimspiderRedisEnv failed[%d]" , nret );
		return -1;
	}
	
	ResetSimSpiderEnv( penv );
	
	nret = AppendRequestQueue( penv , "" , url , 1 ) ;
	if( nret )
	{
		ErrorLog( __FILE__ , __LINE__ , "AppendRequestQueue failed[%d]" , nret );
		return -1;
	}
	else
	{
		InfoLog( __FILE__ , __LINE__ , "AppendRequestQueue ok" );
	}
	
	for( index = 0 ; index < g_process_count ; index++ )
	{
		nret = SetClusterProcessStatus( penv , index , 0 ) ;
		if( nret )
		{
			ErrorLog( __FILE__ , __LINE__ , "redisCommand[%s] failed[%s]" , command , conn->errstr , reply?reply->str:"" );
			freeReplyObject( reply );
			return -1;
		}
		else
		{
			DebugLog( __FILE__ , __LINE__ , "redisCommand[%s] ok" , command );
			freeReplyObject( reply );
		}
	}
	
	UnbindSimspiderRedisQueueHandler( penv );
	
	CleanSimSpiderEnv( & penv );
	
	for( index = 0 ; index < g_process_count ; index++ )
	{
		pids[index] = fork() ;
		if( pids[index] < 0 )
		{
			FatalLog( __FILE__ , __LINE__ , "redisCommand[%s] failed[%s]" , command , conn->errstr , reply?reply->str:"" );
			return -1;
		}
		else if( pids[index] == 0 )
		{
			printf( "[%d]pid[%ld] beginning\n" , index , (long)getpid() );
			nret = simspider_redis( index , ip , port , url , max_concurrent_count ) ;
			printf( "[%d]pid[%ld] ending[%d]\n" , index , (long)getpid() , nret );
			exit( -nret );
		}
	}
	
	for( index = 0 ; index < g_process_count ; index++ )
	{
		waitpid( -1 , & status , 0 );
	}
	
	return 0;
}

static void usage()
{
	printf( "simspider-cluster v%s\n" , __SIMSPIDER_REDIS_VERSION );
	printf( "copyright by calvin<calvinwilliams.c@gmail.com> 2014,2015\n" );
	printf( "USAGE : simspider-cluster ip port url process_count max_concurrent_count\n" );
}

int main( int argc , char *argv[] )
{
	setbuf( stdout , NULL );
	
	if( argc == 1 )
	{
		usage();
		return 0;
	}
	else if( argc == 1 + 5 )
	{
		return -simspider_cluster( argv[1] , atol(argv[2]) , argv[3] , atol(argv[4]) , atol(argv[5]) );
	}
	else
	{
		usage();
		return 7;
	}
}

