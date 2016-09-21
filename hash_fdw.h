#ifndef IBASE_FDW_H
#define IBASE_FDW_H

#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "libpq-fe.h"
#include "utils/relcache.h"
#include "funcapi.h"

#define HASH_TUPLE_COST_MULTIPLIER 10

#define OPTION_KEY        "key"
#define OPTION_HASH_IDX	  "hash_idx"

typedef struct HashFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* for remote query execution */
	PGconn	   *conn;			/* connection for the scan */
	char	   *p_name;			/* name of prepared statement, if created */

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber	ctidAttno;		/* attnum of input resjunk ctid column */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} HashFdwModifyState;

typedef struct HashValidOption
{
    const char *optionName;
    Oid optionContextId;
} HashValidOption;

typedef struct HashFdwOptions
{
	char *column_key;
	char *hash_idx;
} HashFdwOptions;

static const uint32 ValidOptionCount = 2;
static const HashValidOption ValidOptionArray[] =
{
    /* foreign table options */
    { OPTION_KEY, ForeignTableRelationId },
    {OPTION_HASH_IDX,ForeignTableRelationId}
};

extern Datum hash_fdw_handler(PG_FUNCTION_ARGS);
extern Datum hash_fdw_validator(PG_FUNCTION_ARGS);

#endif   /* IBASE_FDW_H */
