#include <unistd.h>
#include <postgres.h>
#include <access/hash.h>
#include <access/xact.h>
#include <funcapi.h>
#include <storage/ipc.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/typcache.h>
#include <catalog/pg_type.h>
#include <catalog/pg_attrdef.h>
#include <catalog/namespace.h>
#include <libpq/pqformat.h>
#include "executor/spi.h"
#include "commands/trigger.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/nabstime.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "tsearch/ts_locale.h"
#include "shm_alloc.h"
//#include "storage/shm_alloc.h"

#if PG_VERSION_NUM>=90503
#include "access/htup_details.h"
#endif
#include "hash_if.h"
#include "executor/executor.h"
#include <catalog/namespace.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(hash_insert_hashdata);
PG_FUNCTION_INFO_V1(hash_get_hashdata);

void hash_shmem_startup(void);
static int shmem_size = 1024;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
//static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
//static test_state_t* test;
static int n_timeseries = 1000000;
static char **hash_name ;
/*static char *hash_name[] = {
  "hash_table1",
  "hash_table2",
  "hash_table3",
  "hash_table4",
  "hash_table5",
  "hash_table6",
  "hash_table7",
  "hash_table8",
};*/
int hash_idx = 8;


Datum hash_insert_hashdata(PG_FUNCTION_ARGS);
Datum hash_get_hashdata(PG_FUNCTION_ARGS);

static uint32 hash_fdw_fn(const void *key, Size keysize)
{
	char const* id = ((hash_key_t*)key)->id;
    uint32 h = 0;
    while (*id != 0) {
        h = h*31 + *id++;
    }
    return h;
}

static int hash_match_fn(const void *key1, const void *key2, Size keysize)
{
    return strcmp(((hash_key_t*)key1)->id, ((hash_key_t*)key2)->id);
}

static void* hash_keycopy_fn(void *dest, const void *src, Size keysize)
{
    hash_key_t* dk = (hash_key_t*)dest;
    hash_key_t* sk = (hash_key_t*)src;
    dk->id = (char*)ShmemAlloc(strlen(sk->id)+1);
    if (dk->id == NULL)
    {
        ereport(ERROR,
               	(errcode(ERRCODE_OUT_OF_MEMORY),
               	 errmsg("Not enough share memry!"))
               	);
    }
    strcpy(dk->id, sk->id);
    return dk;
}


static void _init_hash()
{
    int i=0,j;
    char *hash_tmp_name="hash_index";
    char *n = NULL;
    char tmp[1024] ={0};	
    static HASHCTL info;

	 DefineCustomIntVariable("hash_fdw.hash_idx",
                        "hash_fdw index number.",
                        NULL,
                        &hash_idx,
                        8,
                        1,  
                        INT_MAX,
                        PGC_USERSET,
                        0,  
                        NULL,
                        NULL,
                        NULL);
//	 hash_table_count[hash_idx];
	//HASH_TABLE_COUNT = hash_idx;
	//hash_table_count = (HTAB*)palloc0(sizeof(HTAB*)* hash_idx);

	hash_name = palloc0(sizeof(char *) *hash_idx+1);

	for(j=1;j<=hash_idx;j++){
	n = palloc0(sizeof(char *)*2);
	sprintf(n,"%d",j);

	hash_name[j-1] = palloc0(strlen(hash_tmp_name)+strlen(n)+1);
	strcpy(tmp,hash_tmp_name);
	strcat(tmp,n);	
	
	memcpy(hash_name[j-1],tmp,strlen(tmp)+1);
	memset(tmp,0,sizeof(tmp));
	pfree(n);

	}

	info.keysize = sizeof(hash_key_t);
	info.entrysize = sizeof(hash_entry_t);
	info.hash = hash_fdw_fn;
	info.match = hash_match_fn;
	info.keycopy = hash_keycopy_fn;
    for(i = 0; i < hash_idx; i++)
    {
    	hash_table_count[i] = ShmemInitHash(hash_name[i],
        							 n_timeseries, n_timeseries*10,
        							 &info,
        							 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_KEYCOPY);
    }
}

hash_value_t* hash_get_data(int hash_index, char const* key_id)
{
    hash_value_t* tv;
    hash_entry_t* entry;
    hash_key_t key;

    if (key_id == NULL)
    {
        return NULL;
    }

    key.id = (char*)key_id;
    /* Find or create an entry with desired hash code */
    entry = (hash_entry_t*)hash_search(hash_table_count[hash_index - 1], &key, HASH_FIND, NULL);

if (NULL == entry)
    {
/**
       	ereport(ERROR,
       	(errcode(ERRCODE_OUT_OF_MEMORY),
       	 errmsg("Not find the key value in this table!"))
       	);
***/
        return NULL;
    }
    else
    {
        tv = &entry->value;
    }

    return tv;
}

void hash_shmem_startup(void)
{
    int i;
	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
    }

	/* reset in case this is a restart within the postmaster */
	for (i = 0; i <= hash_idx; i++)
    {
	    hash_table_count[i] = NULL;
    }
	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    _init_hash();
	LWLockRelease(AddinShmemInitLock);
}

static void hash_executor_end(QueryDesc *queryDesc)
{
	standard_ExecutorEnd(queryDesc);
}

hash_value_t* hash_insert_data(int hash_index, char const* key_id, HeapTupleHeader tuplehead)
{
	hash_value_t* ts;
    hash_entry_t* entry;
    hash_key_t key;
	bool found;
    uint32 ulen;
    HeapTupleHeader tmpTuple;

    if (key_id == NULL)
    {
        return NULL;
    }

    /* 得到tuple数据长度 */
    ulen = HeapTupleHeaderGetDatumLength(tuplehead);
    
    key.id = (char*)key_id;
    /* Find or create an entry with desired hash code */
    entry = (hash_entry_t*)hash_search(hash_table_count[hash_index - 1], &key, HASH_ENTER, &found);
    if (NULL == entry)
    {
       	ereport(ERROR,
               	(errcode(ERRCODE_OUT_OF_MEMORY),
               	 errmsg("Not enough share memry!"))
               	);
        return NULL;
    }
    else
    {
	    ts = &entry->value;
	    if (ts->t_data != NULL)
	    {
		    free(ts->t_data);
		    ts->t_data = NULL;
	    }
	    /* 分配shamem并copy TupleData */
	    tmpTuple = (HeapTupleHeader) malloc(ulen);
	    //tmpTuple = (HeapTupleHeader) ShmemAlloc(ulen);
	    if (tmpTuple == NULL)
	    {
		    ereport(WARNING,
				    (errcode(ERRCODE_OUT_OF_MEMORY),
				     errmsg("Not enough share memry!"))
			   );
		    return NULL;
	    }
	    memcpy(tmpTuple, tuplehead, ulen);
	    ts->t_data = tmpTuple;
    }
    return ts;
}

void hash_put_shmhandle(int hash_index, char const* key_id, void *data)
{
	hash_value_t* ts;
    hash_entry_t* entry;
    hash_key_t key;
	bool found;

    if (key_id == NULL)
    {
        return;
    }

    key.id = (char*)key_id;
    /* Find or create an entry with desired hash code */
    entry = (hash_entry_t*)hash_search(hash_table_count[hash_index - 1], &key, HASH_ENTER, &found);
    if (NULL == entry)
    {
       	ereport(ERROR,
               	(errcode(ERRCODE_OUT_OF_MEMORY),
               	 errmsg("Not enough share memry!"))
               	);
        return;
    }
    else
    {
        ts = &entry->value;
		if (ts->t_data != NULL)
		{
			ShmemDynFree(ts->t_data);
			ts->t_data = NULL;
		}

        ts->t_data = (HeapTupleHeader)data;
    }

    return;
}

void *hash_get_shmhandle(int hash_index, char const* key_id)
{
	hash_value_t* tv;
    hash_entry_t* entry;
    hash_key_t key;

    if (key_id == NULL)
    {
        return NULL;
    }

    key.id = (char*)key_id;
    /* Find or create an entry with desired hash code */
    entry = (hash_entry_t*)hash_search(hash_table_count[hash_index - 1], &key, HASH_FIND, NULL);

if (NULL == entry)
    {
/**
       	ereport(ERROR,
       	(errcode(ERRCODE_OUT_OF_MEMORY),
       	 errmsg("Not find the key value in this table!"))
       	);
***/
        return NULL;
    }
    else
    {
        tv = &entry->value;
    }

    return tv->t_data;
}

void		_PG_init(void);
void		_PG_fini(void);

void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	DefineCustomIntVariable("hash.shmem_size",
                            "Size of shared memory (Mb) used by hash store.",
							NULL,
							&shmem_size,
							1*1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
							
//elog(LOG,"test!!!,%d\n",shmem_size);
	RequestAddinShmemSpace((size_t)shmem_size*MB);
	RequestAddinLWLocks(1);
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = hash_shmem_startup;
}
void _PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = NULL;
}

Datum hash_insert_hashdata(PG_FUNCTION_ARGS)
{
    hash_value_t *tv;
    HeapTupleHeader tuplehead = NULL;

    int hash_index = PG_GETARG_INT32(0);
    char const* key_id = PG_GETARG_CSTRING(1);
    tuplehead = PG_GETARG_HEAPTUPLEHEADER(2);

    if (hash_index < 1 || hash_index > 8)
    {
       	ereport(ERROR,
           	(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
           	 errmsg("input hash table index wrong!"))
           	);

        PG_RETURN_BOOL(0);
    }

    /* 插入操作 */
    tv = hash_insert_data(hash_index, key_id, tuplehead);
    if (tv == NULL)
    {
        PG_RETURN_BOOL(0);
    }

    PG_RETURN_BOOL(1);
}

Datum hash_get_hashdata(PG_FUNCTION_ARGS)
{
    HeapTupleHeader tuple;
    hash_value_t *tv;
    int hash_index = PG_GETARG_INT32(0);
    char const* key_id = PG_GETARG_CSTRING(1);

    if (hash_index < 1 || hash_index > 8)
    {
       	ereport(ERROR,
               	(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
               	 errmsg("input hash table index wrong!"))
               	);

        PG_RETURN_NULL();
    }

    /* get操作 */
    tv = hash_get_data(hash_index, key_id);
    if (tv == NULL)
    {
        PG_RETURN_NULL();
    }
    else
    {
        tuple = tv->t_data;
    }

    PG_RETURN_HEAPTUPLEHEADER(tuple);
}

void hash_del_data(int hash_index,char *key_id)
{
        //int hash_index = PG_GETARG_INT32(0);
        //char *key_id = text_to_cstring(PG_GETARG_TEXT_P(1));

        HASH_SEQ_STATUS scan_status;
        hash_entry_t* entry;

        HTAB *elements_tab = hash_table_count[hash_index -1]; 

        hash_seq_init(&scan_status, elements_tab);
        while ((entry = (hash_entry_t *) hash_seq_search(&scan_status)) != NULL)
        {    

                if(!strcmp(key_id,entry->key.id))
                {    

                        if (hash_search(elements_tab, (const void *) &entry->key,
                                                HASH_REMOVE, NULL) == NULL)
                                elog(LOG, "hash table corrupted");
                        if(entry->key.id != NULL)
                        {    
                                ShmemDynFree(entry->key.id);
                        }    
                        hash_seq_term(&scan_status); 
                        break; 
                }    

        }    
    //    PG_RETURN_TEXT_P(cstring_to_text("delete hash key  finish"));     

}



