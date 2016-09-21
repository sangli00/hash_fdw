#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include "postgres.h"
#include "hash_fdw.h"
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/jsonb.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "utils/rel.h"

#include "hash_if.h"
PG_FUNCTION_INFO_V1(hash_fdw_handler);
PG_FUNCTION_INFO_V1(hash_fdw_validator);

static HashFdwOptions *HashGetOptions(Oid foreignTableId);
static void HashGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static void HashGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static ForeignScan * HashGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
                                      ForeignPath *bestPath, List *targetList, List *scanClauses,Plan *outer_plan);
static List *HashPlanForeignModify(PlannerInfo *plannerInfo, ModifyTable *plan,
                                 Index resultRelation, int subplanIndex);
static void HashBeginForeignModify(ModifyTableState *modifyTableState,
                                 ResultRelInfo *relationInfo, List *fdwPrivate,
                                 int subplanIndex, int executorFlags);
static TupleTableSlot *HashExecForeignInsert(EState *executorState, ResultRelInfo *relationInfo,
                                           TupleTableSlot *tupleSlot, TupleTableSlot *planSlot);
static void HashEndForeignModify(EState *executorState, ResultRelInfo *relationInfo);

static void BeginScanHashRelation(ForeignScanState *scanState, int executorFlags);

static TupleTableSlot *hashExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo,
                                                                                          TupleTableSlot *slot, TupleTableSlot *planSlot);

static TupleTableSlot *HashScanNext(ForeignScanState *scanState);

static void EndScanHashRelation(ForeignScanState *scanState);

static List *ColumnList(RelOptInfo *baserel);

Datum hash_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

    fdwRoutine->GetForeignRelSize = HashGetForeignRelSize;
    fdwRoutine->GetForeignPaths = HashGetForeignPaths;
    fdwRoutine->GetForeignPlan = HashGetForeignPlan;

    /* hash_fdw scan function */
    fdwRoutine->BeginForeignScan = BeginScanHashRelation;
    fdwRoutine->IterateForeignScan = HashScanNext;
    fdwRoutine->EndForeignScan = EndScanHashRelation;

    /* hash_fdw insert function */
    fdwRoutine->PlanForeignModify = HashPlanForeignModify;
    fdwRoutine->BeginForeignModify = HashBeginForeignModify;
    fdwRoutine->ExecForeignInsert = HashExecForeignInsert;
    fdwRoutine->EndForeignModify = HashEndForeignModify;
  
    fdwRoutine->ExecForeignDelete = hashExecForeignDelete;

    PG_RETURN_POINTER(fdwRoutine);
}

static StringInfo OptionNamesString(Oid currentContextId)
{
    StringInfo optionNamesString = makeStringInfo();
    bool firstOptionAppended = false;

    int32 optionIndex = 0;
    for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
    {
        const HashValidOption *validOption = &(ValidOptionArray[optionIndex]);

        /* if option belongs to current context, append option name */
        if (currentContextId == validOption->optionContextId)
        {
            if (firstOptionAppended)
            {
                appendStringInfoString(optionNamesString, ", ");
            }

            appendStringInfoString(optionNamesString, validOption->optionName);
            firstOptionAppended = true;
        }
    }

	return optionNamesString;
}


Datum hash_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum optionArray = PG_GETARG_DATUM(0);
	Oid optionContextId = PG_GETARG_OID(1);
	List *optionList = untransformRelOptions(optionArray);
	ListCell *optionCell = NULL;
//	char *column_key = NULL;
//	char *hash_idx = NULL;
	if(optionList->length != ValidOptionCount ){
	 ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option size need equal %d", ValidOptionCount),
                    errhint("Valid options in this context are: key 'columnName',hash_idx = [1-%d]",hash_idx)));
                }   


	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionName = optionDef->defname;
		bool optionValid = false;

		if(strcmp(optionName , OPTION_HASH_IDX) == 0){
 			//Value *val = optionDef->arg;
			if( atoi(strVal(optionDef->arg)) > hash_idx){
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option \"%s\"", optionName),
						 errhint("Valid options \"hash_idx\" need less than or equal to: %d", hash_idx)));
			}
		}

		int32 optionIndex = 0;
		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
	        const HashValidOption *validOption = &(ValidOptionArray[optionIndex]);

	        if ((optionContextId == validOption->optionContextId)
	                        && (strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
	        {
                optionValid = true;
                break;
	        }
		}

		/* if invalid option, display an informative error message */
		if (!optionValid)
		{
	        StringInfo optionNamesString = OptionNamesString(optionContextId);

	        ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option \"%s\"", optionName),
                    errhint("Valid options in this context are: %s", optionNamesString->data)));
		}
	//	optionIndex++;
/*
		if (strncmp(optionName, OPTION_KEY, NAMEDATALEN) == 0)
		{
			column_key = defGetString(optionDef);
		}

		if(strncmp(optionName,OPTION_HASH_IDX,NAMEDATALEN) == 0)
		{
			hash_idx = defGetString(optionDef);	
		}*/
	}

    if (optionContextId == ForeignTableRelationId)
    {
		/* FIXME: check option list is valid */
    }

    PG_RETURN_VOID() ;
}


static char *HashGetOptionValue(Oid foreignTableId, const char *optionName)
{
    ForeignTable *foreignTable = NULL;
    ForeignServer *foreignServer = NULL;
    List *optionList = NIL;
    ListCell *optionCell = NULL;
    char *optionValue = NULL;

    foreignTable = GetForeignTable(foreignTableId);
    foreignServer = GetForeignServer(foreignTable->serverid);

    optionList = list_concat(optionList, foreignTable->options);
    optionList = list_concat(optionList, foreignServer->options);

    foreach(optionCell, optionList)
    {
        DefElem *optionDef = (DefElem *) lfirst(optionCell);
        char *optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
        {
            optionValue = defGetString(optionDef);
            break;
        }
    }

    return optionValue;
}

static HashFdwOptions *HashGetOptions(Oid foreignTableId)
{
    HashFdwOptions *Options = NULL;
    char *column_key = NULL;
    char *hash_idx = NULL;

    column_key = HashGetOptionValue(foreignTableId, OPTION_KEY);
    hash_idx = HashGetOptionValue(foreignTableId, OPTION_HASH_IDX);

	/* FIXME: check option list is valid */
    Options = (HashFdwOptions *)palloc0(sizeof(HashFdwOptions));
	Options->column_key = palloc0(strlen(column_key) + 1);
	Options->hash_idx =  palloc0(strlen(hash_idx) + 1);
    memcpy(Options->column_key, column_key, strlen(column_key));
    memcpy(Options->hash_idx, hash_idx, strlen(hash_idx));

    return Options;
}

static List *ColumnList(RelOptInfo *baserel)
{
    List *columnList = NIL;
    List *neededColumnList = NIL;
    AttrNumber columnIndex = 1;
    AttrNumber columnCount = baserel->max_attr;
    List *targetColumnList = baserel->reltargetlist;
    List *restrictInfoList = baserel->baserestrictinfo;
    ListCell *restrictInfoCell = NULL;

    /* first add the columns used in joins and projections */
    neededColumnList = list_copy(targetColumnList);

    /* then walk over all restriction clauses, and pull up any used columns */
    foreach(restrictInfoCell, restrictInfoList)
    {
        RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Node *restrictClause = (Node *) restrictInfo->clause;
        List *clauseColumnList = NIL;

        /* recursively pull up any columns used in the restriction clause */
        clauseColumnList = pull_var_clause(restrictClause, PVC_RECURSE_AGGREGATES,
                        PVC_RECURSE_PLACEHOLDERS);

        neededColumnList = list_union(neededColumnList, clauseColumnList);
    }
    /* walk over all column definitions, and de-duplicate column list */
    for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
    {
        ListCell *neededColumnCell = NULL;
        Var *column = NULL;

        /* look for this column in the needed column list */
        foreach(neededColumnCell, neededColumnList)
        {
            Var *neededColumn = (Var *) lfirst(neededColumnCell);
            if (neededColumn->varattno == columnIndex)
            {
                column = neededColumn;
                break;
            }
        }

        if (column != NULL)
        {
            columnList = lappend(columnList, column);
        }
    }

    return columnList;
}

static void HashGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
    return;
}

static void HashGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
    Path *foreignScanPath = NULL;
#if 0
    OrcFdwOptions *options = OrcGetOptions(foreignTableId);

    BlockNumber pageCount = PageCount(options->filename);
    double tupleCount = TupleCount(baserel, options->filename);
#endif

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan to account for
     * the cost of parsing records.
     */
    //double tupleParseCost = cpu_tuple_cost * HASH_TUPLE_COST_MULTIPLIER;
    //double tupleFilterCost = baserel->baserestrictcost.per_tuple;
    //double cpuCostPerTuple = tupleParseCost + tupleFilterCost;
    //double executionCost = (seq_page_cost * pageCount) + (cpuCostPerTuple * tupleCount);

    double startupCost = baserel->baserestrictcost.startup;
    //double totalCost = startupCost + executionCost;
    Cost startup_cost;
    Cost total_cost;
    startup_cost = baserel->baserestrictcost.startup;
    total_cost = startup_cost + (cpu_tuple_cost * baserel->rows);


    /* create a foreign path node and add it as the only possible path */
    foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows, startupCost,
                    //startupCost,
		    total_cost,
                    NIL, /* no known ordering */
                    NULL, /* not parameterized */
                    NIL,NIL); /* no fdw_private */

    add_path(baserel, foreignScanPath);

    return;
}

static ForeignScan * HashGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
                                      ForeignPath *bestPath, List *targetList, List *scanClauses,Plan *outer_plan)
{
    ForeignScan *foreignScan = NULL;
    List *columnList = NULL;
    List *foreignPrivateList = NIL;
    /*
    * We have no native ability to evaluate restriction clauses, so we just
    * put all the scanClauses into the plan node's qual list for the executor
    * to check.
    */
    scanClauses = extract_actual_clauses(scanClauses, false);
    /*
    * As an optimization, we only add columns that are present in the query to
    * the column mapping hash. To find these columns, we need baserel. We don't
    * have access to baserel in executor's callback functions, so we get the
    * column list here and put it into foreign scan node's private list.
    */
    columnList = ColumnList(baserel);
    foreignPrivateList = list_make1(columnList);

    /* create the foreign scan node */
    foreignScan = make_foreignscan(targetList, scanClauses, baserel->relid,
    NIL, /* no expressions to evaluate */
    foreignPrivateList,
	NIL,NIL,outer_plan);

    return foreignScan;
}

static void HashBeginForeignModify(ModifyTableState *modifyTableState,
                                 ResultRelInfo *relationInfo, List *fdwPrivate,
                                 int subplanIndex, int executorFlags)
{
	Oid  foreignTableOid = InvalidOid;
	HashFdwOptions *Options = NULL;

	foreignTableOid = RelationGetRelid(relationInfo->ri_RelationDesc);

	Options = HashGetOptions(foreignTableOid);
	relationInfo->ri_FdwState = (void *)Options;

	return;
}

static TupleTableSlot *HashExecForeignInsert(EState *executorState, ResultRelInfo *relationInfo,
                                           TupleTableSlot *tupleSlot, TupleTableSlot *planSlot)
{
	int i;
	char *val;
	TupleDesc tupleDescriptor;
	int column_count = 0;
	HashFdwOptions *Options = NULL;
    Oid             typoutputfunc;
    bool            typIsVarlena;

	if (tupleSlot->tts_tuple != NULL)
	{
	    if(HeapTupleHasExternal(tupleSlot->tts_tuple))
	    {
	        /* detoast any toasted attributes */
	        tupleSlot->tts_tuple = toast_flatten_tuple(tupleSlot->tts_tuple,
	                tupleSlot->tts_tupleDescriptor);
	    }
	}
       slot_getallattrs(tupleSlot);

	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Options = (HashFdwOptions *) relationInfo->ri_FdwState;
	column_count = tupleDescriptor->natts;
	int hash_idx = atoi(Options->hash_idx);
	
    for(i = 0; i < column_count; i++)
    {
        if(strcmp(Options->column_key, (char *)(tupleDescriptor->attrs[i]->attname.data)) == 0)
        {
            getTypeOutputInfo(tupleDescriptor->attrs[i]->atttypid, &typoutputfunc, &typIsVarlena);
            val = DatumGetCString(OidFunctionCall1(typoutputfunc, tupleSlot->tts_values[i]));
            break;
        }
    }

    hash_insert_data(hash_idx, val, tupleSlot->tts_tuple->t_data);

	return tupleSlot;
}

static void HashEndForeignModify(EState *executorState, ResultRelInfo *relationInfo)
{
    return;
}

static List *HashPlanForeignModify(PlannerInfo *plannerInfo, ModifyTable *plan,
                                 Index resultRelation, int subplanIndex)
{
    bool operationSupported = false;

    if (plan->operation == CMD_INSERT)
    {
        ListCell *tableCell = NULL;
        Query *query = NULL;

        /*
         *       * Only insert operation with select subquery is supported. Other forms
         *               * of insert, update, and delete operations are not supported.
         *                       */
        query = plannerInfo->parse;
        foreach(tableCell, query->rtable)
        {
            RangeTblEntry *tableEntry = lfirst(tableCell);

            if (tableEntry->rtekind == RTE_SUBQUERY &&
                    tableEntry->subquery != NULL &&
                    tableEntry->subquery->commandType == CMD_SELECT)
            {
                operationSupported = true;
                break;
            }
        }
    }

	#if 0
    if (!operationSupported)
    {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("operation is not supported")));
    }
	#endif
    return NULL;
}

static void BeginScanHashRelation(ForeignScanState *scanState, int executorFlags)
{
    #if 0
    int ret;
    pthread_t id;
    as_context *context;
	Oid foreignTableId = InvalidOid;
	TupleDesc	tupleDescriptor;

	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	tupleDescriptor = scanState->ss.ss_ScanTupleSlot ->tts_tupleDescriptor;

    context = palloc0(sizeof(as_context));

    context->context= cf_queue_create(sizeof(pg_value *), true);
	context->foreignTableId = foreignTableId;
	context->tupleDescriptor = tupleDescriptor;

    ret = pthread_create(&id, NULL, As_thread_task, (void*)context);

    #endif
    HASH_SEQ_STATUS *scan_status;
    scan_status = palloc0(sizeof(HASH_SEQ_STATUS));
	Oid foreignTableId;
	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);

	int hash_idx =  atoi(HashGetOptionValue(foreignTableId,OPTION_HASH_IDX));

    hash_seq_init(scan_status, hash_table_count[hash_idx - 1]);

    scanState->fdw_state = (void *)scan_status;
    return;
}

static TupleTableSlot *HashScanNext(ForeignScanState *scanState)
{
    #if 0
    as_context *context;
    pg_value *pgvalue = NULL;
    TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
    Datum *columnValues = tupleSlot->tts_values;
    bool *columnNulls = tupleSlot->tts_isnull;
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    int columnCount = tupleDescriptor->natts;

    /* initialize all values for this row to null */
    memset(columnValues, 0, columnCount * sizeof(Datum));
    memset(columnNulls, true, columnCount * sizeof(bool));

    ExecClearTuple(tupleSlot);


    cf_queue_pop(context->context, &pgvalue, -1);

    if (pgvalue->isover == true)
    {
        pthread_join(context->id, NULL);
        if (context->context)
        {
            cf_queue_destroy(context->context);
            context->context = NULL;
        }
    }
    else
    {
        Fill_Tupleslot(columnValues, columnNulls, pgvalue,
                       g_compile_result->listlen,
                       g_compile_result->tableid,
                       context->tupleDescriptor);
        free_pgvalue(pgvalue);
        ExecStoreVirtualTuple(tupleSlot);
    }

    return tupleSlot;
    #endif
    HASH_SEQ_STATUS *scan_status;
    hash_entry_t *entry;
    HeapTupleHeader tuphdr;
    HeapTupleData tmptup;

    TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
    Datum *columnValues = tupleSlot->tts_values;
    bool *columnNulls = tupleSlot->tts_isnull;
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    scan_status = (HASH_SEQ_STATUS *)scanState->fdw_state;
    ExecClearTuple(tupleSlot);

    if ((entry = (hash_entry_t *) hash_seq_search(scan_status)) != NULL)
    {
        tuphdr = entry->value.t_data;
        tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = tuphdr;

        heap_deform_tuple(&tmptup, tupleDescriptor, columnValues, columnNulls);
        ExecStoreVirtualTuple(tupleSlot);
        /* We should free memory if element is not passed by value */
    }

    return tupleSlot;

}

static void EndScanHashRelation(ForeignScanState *scanState)
{
    return;
}

static TupleTableSlot *
hashExecForeignDelete(EState *estate,
                                           ResultRelInfo *resultRelInfo,
                                           TupleTableSlot *slot,
                                           TupleTableSlot *planSlot){
	/*Datum           value ;
	bool		isNull;
	char	*x;
	List   *rtable; 
	RangeTblEntry *rt;
	HashFdwOptions *option;	
	
	rtable = estate->es_plannedstmt->rtable;
	rt = lfirst(rtable->head);

	option = HashGetOptions(rt->relid);
	
	value = ExecGetJunkAttribute(planSlot, 1, &isNull);
*/
	//HashFdwModifyState *fmstate = (HashFdwModifyState *) resultRelInfo->ri_FdwState;
	//PgFdwModifyState *fmstate = (PgFdwModifyState *) resultRelInfo->ri_FdwState;
	//value = ExecGetJunkAttribute(planSlot, 1, &isNull);
	
}


