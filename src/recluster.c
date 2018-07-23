/*-------------------------------------------------------------------------
 *
 * cluster.c
 *
 * CLUSTER command which only blocks concurrent writes not reads.
 * Based on an edited version of src/backend/commands/cluster.c from
 * postgresql-10.4 ab5e9caa4a3ec4765348a0482e88edcf3f6aab4a
 *
 * original copyright:
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_am.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/tuplesort.h"

#include "chunk_index.h"
#include "recluster.h"

static void timescale_rebuild_relation(Relation OldHeap, Oid indexOid, bool verbose);
static void copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex,
			   bool verbose, bool *pSwapToastByContent,
			   TransactionId *pFreezeXid, MultiXactId *pCutoffMulti);

static void reform_and_rewrite_tuple(HeapTuple tuple,
						 TupleDesc oldTupDesc, TupleDesc newTupDesc,
						 Datum *values, bool *isnull,
						 bool newRelHasOids, RewriteState rwstate);

static void finish_heap_swaps(Oid OIDOldHeap, Oid OIDNewHeap,
				 List *new_index_oids,
				 bool swap_toast_by_content,
				 bool is_internal,
				 TransactionId frozenXid,
				 MultiXactId cutoffMulti);

static void swap_relation_files(Oid r1, Oid r2,
					bool swap_toast_by_content,
					bool is_internal,
					TransactionId frozenXid,
					MultiXactId cutoffMulti);

/*
 * timescale_cluster_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenodes of the new table and the old table, so
 * the OID of the original table is preserved.  Thus we do not lose
 * GRANT, inheritance nor references to this table (this was a bug
 * in releases through 7.3).
 *
 * Indexes are rebuilt in the same manner.
 *
 * If indexOid is InvalidOid, the table will be rewritten in physical order
 * instead of index order.
 */
void
timescale_recluster_rel(Oid tableOid, Oid indexOid, bool recheck, bool verbose)
{
	Relation	OldHeap;

	if(!OidIsValid(indexOid))
		elog(ERROR, "Recluster must specify an index.");

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/*
	 * We grab exclusive access to the target rel and index for the duration
	 * of the transaction.  (This is redundant for the single-transaction
	 * case, since cluster() already did it.)  The index lock is taken inside
	 * check_index_is_clusterable.
	 */
	OldHeap = try_relation_open(tableOid, ExclusiveLock);

	/* If the table has gone away, we can skip processing it */
	if (!OldHeap)
		return;

	/*
	 * Since we may open a new transaction for each relation, we have to check
	 * that the relation still is what we think it is.
	 *
	 * If this is a single-transaction CLUSTER, we can skip these tests. We
	 * *must* skip the one on indisclustered since it would reject an attempt
	 * to cluster a not-previously-clustered index.
	 */
	if (recheck)
	{
		HeapTuple	tuple;
		Form_pg_index indexForm;

		/* Check that the user still owns the relation */
		if (!pg_class_ownercheck(tableOid, GetUserId()))
		{
			relation_close(OldHeap, ExclusiveLock);
			return;
		}

		/*
		 * Silently skip a temp table for a remote session.  Only doing this
		 * check in the "recheck" case is appropriate (which currently means
		 * somebody is executing a database-wide CLUSTER), because there is
		 * another check in cluster() which will stop any attempt to cluster
		 * remote temp tables by name.  There is another check in timescale_cluster_rel
		 * which is redundant, but we leave it for extra safety.
		 */
		if (RELATION_IS_OTHER_TEMP(OldHeap))
		{
			relation_close(OldHeap, ExclusiveLock);
			return;
		}

		if (OidIsValid(indexOid))
		{
			/*
			 * Check that the index still exists
			 */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid)))
			{
				relation_close(OldHeap, ExclusiveLock);
				return;
			}

			/*
			 * Check that the index is still the one with indisclustered set.
			 */
			tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
			if (!HeapTupleIsValid(tuple))	/* probably can't happen */
			{
				relation_close(OldHeap, ExclusiveLock);
				return;
			}
			indexForm = (Form_pg_index) GETSTRUCT(tuple);
			if (!indexForm->indisclustered)
			{
				ReleaseSysCache(tuple);
				relation_close(OldHeap, ExclusiveLock);
				return;
			}
			ReleaseSysCache(tuple);
		}
	}

	/*
	 * We allow VACUUM FULL, but not CLUSTER, on shared catalogs.  CLUSTER
	 * would work in most respects, but the index would only get marked as
	 * indisclustered in the current database, leading to unexpected behavior
	 * if CLUSTER were later invoked in another database.
	 */
	if (OldHeap->rd_rel->relisshared)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster a shared catalog")));

	/*
	 * Don't process temp tables of other backends ... their local buffer
	 * manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(OldHeap))
	{
		if (OidIsValid(indexOid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot cluster temporary tables of other sessions")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot vacuum temporary tables of other sessions")));
	}

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	//TODO is this check needed/valid?
	CheckTableNotInUse(OldHeap, "CLUSTER");

	/* Check heap and index are valid to cluster on */
	check_index_is_clusterable(OldHeap, indexOid, recheck, ExclusiveLock);

	/*
	 * Quietly ignore the request if this is a materialized view which has not
	 * been populated from its query. No harm is done because there is no data
	 * to deal with, and we don't want to throw an error if this is part of a
	 * multi-relation request -- for example, CLUSTER was run on the entire
	 * database.
	 */
	if (OldHeap->rd_rel->relkind == RELKIND_MATVIEW &&
		!RelationIsPopulated(OldHeap))
	{
		relation_close(OldHeap, ExclusiveLock);
		return;
	}

	/*
	 * All predicate locks on the tuples or pages are about to be made
	 * invalid, because we move tuples around.  Promote them to relation
	 * locks.  Predicate locks on indexes will be promoted when they are
	 * reindexed.
	 */
	TransferPredicateLocksToHeapRelation(OldHeap);

	/* timescale_rebuild_relation does all the dirty work */
	timescale_rebuild_relation(OldHeap, indexOid, verbose);

	/* NB: timescale_rebuild_relation does heap_close() on OldHeap */
}

/*
 * timescale_rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild --- must be opened and exclusive-locked!
 * indexOid: index to cluster by, or InvalidOid to rewrite in physical order.
 *
 * NB: this routine closes OldHeap at the right time; caller should not.
 */
static void
timescale_rebuild_relation(Relation OldHeap, Oid indexOid, bool verbose)
{
	Oid			tableOid = RelationGetRelid(OldHeap);
	Oid			tableSpace = OldHeap->rd_rel->reltablespace;
	Oid			OIDNewHeap;
	List 		*new_index_oids;
	char		relpersistence;
	bool		swap_toast_by_content;
	TransactionId frozenXid;
	MultiXactId cutoffMulti;

	/* Mark the correct index as clustered */
	mark_index_clustered(OldHeap, indexOid, true);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;
	if(IsSystemRelation(OldHeap))
		elog(ERROR, "Cannot recluster a system catalog.");

	/* Close relcache entry, but keep lock until transaction commit */
	heap_close(OldHeap, NoLock);

	/* Create the transient table that will receive the re-ordered data */
	OIDNewHeap = make_new_heap(tableOid, tableSpace,
							   relpersistence,
							   ExclusiveLock);

	/* Copy the heap data into the new table in the desired order */
	copy_heap_data(OIDNewHeap, tableOid, indexOid, verbose,
				   &swap_toast_by_content, &frozenXid, &cutoffMulti);

	/* Create versions of the tables indexes for the new table */
	new_index_oids = chunk_index_duplicate(tableOid, OIDNewHeap);

	/*
	 * Swap the physical files of the target and transient tables, then
	 * rebuild the target's indexes and throw away the transient table.
	 */
	finish_heap_swaps(tableOid, OIDNewHeap, new_index_oids,
					 swap_toast_by_content, true,
					 frozenXid, cutoffMulti);
}

/*
 * Do the physical copying of heap data.
 *
 * There are three output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 * *pCutoffMulti receives the MultiXactId used as a cutoff point.
 */
static void
copy_heap_data(Oid OIDNewHeap, Oid OIDOldHeap, Oid OIDOldIndex, bool verbose,
			   bool *pSwapToastByContent, TransactionId *pFreezeXid,
			   MultiXactId *pCutoffMulti)
{
	Relation	NewHeap,
				OldHeap,
				OldIndex;
	Relation	relRelation;
	HeapTuple	reltup;
	Form_pg_class relform;
	TupleDesc	oldTupDesc;
	TupleDesc	newTupDesc;
	int			natts;
	Datum	   *values;
	bool	   *isnull;
	IndexScanDesc indexScan;
	HeapScanDesc heapScan;
	bool		use_wal;
	TransactionId OldestXmin;
	TransactionId FreezeXid;
	MultiXactId MultiXactCutoff;
	RewriteState rwstate;
	bool		use_sort;
	Tuplesortstate *tuplesort;
	double		num_tuples = 0,
				tups_vacuumed = 0,
				tups_recently_dead = 0;
	BlockNumber num_pages;
	int			elevel = verbose ? INFO : DEBUG2;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	/*
	 * Open the relations we need.
	 */
	NewHeap = heap_open(OIDNewHeap, AccessExclusiveLock);
	OldHeap = heap_open(OIDOldHeap, ExclusiveLock);
	if (OidIsValid(OIDOldIndex))
		OldIndex = index_open(OIDOldIndex, ExclusiveLock);
	else
		OldIndex = NULL;

	/*
	 * Their tuple descriptors should be exactly alike, but here we only need
	 * assume that they have the same number of columns.
	 */
	oldTupDesc = RelationGetDescr(OldHeap);
	newTupDesc = RelationGetDescr(NewHeap);
	Assert(newTupDesc->natts == oldTupDesc->natts);

	/* Preallocate values/isnull arrays */
	natts = newTupDesc->natts;
	values = (Datum *) palloc(natts * sizeof(Datum));
	isnull = (bool *) palloc(natts * sizeof(bool));

	/*
	 * If the OldHeap has a toast table, get lock on the toast table to keep
	 * it from being vacuumed.  This is needed because autovacuum processes
	 * toast tables independently of their main tables, with no lock on the
	 * latter.  If an autovacuum were to start on the toast table after we
	 * compute our OldestXmin below, it would use a later OldestXmin, and then
	 * possibly remove as DEAD toast tuples belonging to main tuples we think
	 * are only RECENTLY_DEAD.  Then we'd fail while trying to copy those
	 * tuples.
	 *
	 * We don't need to open the toast relation here, just lock it.  The lock
	 * will be held till end of transaction.
	 */
	if (OldHeap->rd_rel->reltoastrelid)
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, ExclusiveLock);

	/*
	 * We need to log the copied data in WAL iff WAL archiving/streaming is
	 * enabled AND it's a WAL-logged rel.
	 */
	use_wal = XLogIsNeeded() && RelationNeedsWAL(NewHeap);

	/* use_wal off requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(NewHeap) == InvalidBlockNumber);

	/*
	 * If both tables have TOAST tables, perform toast swap by content.  It is
	 * possible that the old table has a toast table but the new one doesn't,
	 * if toastable columns have been dropped.  In that case we have to do
	 * swap by links.  This is okay because swap by content is only essential
	 * for system catalogs, and we don't support schema changes for them.
	 */
	if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid)
	{
		*pSwapToastByContent = true;

		/*
		 * When doing swap by content, any toast pointers written into NewHeap
		 * must use the old toast table's OID, because that's where the toast
		 * data will eventually be found.  Set this up by setting rd_toastoid.
		 * This also tells toast_save_datum() to preserve the toast value
		 * OIDs, which we want so as not to invalidate toast pointers in
		 * system catalog caches, and to avoid making multiple copies of a
		 * single toast value.
		 *
		 * Note that we must hold NewHeap open until we are done writing data,
		 * since the relcache will not guarantee to remember this setting once
		 * the relation is closed.  Also, this technique depends on the fact
		 * that no one will try to read from the NewHeap until after we've
		 * finished writing it and swapping the rels --- otherwise they could
		 * follow the toast pointers to the wrong place.  (It would actually
		 * work for values copied over from the old toast table, but not for
		 * any values that we toast which were previously not toasted.)
		 */
		NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
	}
	else
		*pSwapToastByContent = false;

	/*
	 * Compute xids used to freeze and weed out dead tuples and multixacts.
	 * Since we're going to rewrite the whole table anyway, there's no reason
	 * not to be aggressive about this.
	 */
	vacuum_set_xid_limits(OldHeap, 0, 0, 0, 0,
						  &OldestXmin, &FreezeXid, NULL, &MultiXactCutoff,
						  NULL);

	/*
	 * FreezeXid will become the table's new relfrozenxid, and that mustn't go
	 * backwards, so take the max.
	 */
	if (TransactionIdPrecedes(FreezeXid, OldHeap->rd_rel->relfrozenxid))
		FreezeXid = OldHeap->rd_rel->relfrozenxid;

	/*
	 * MultiXactCutoff, similarly, shouldn't go backwards either.
	 */
	if (MultiXactIdPrecedes(MultiXactCutoff, OldHeap->rd_rel->relminmxid))
		MultiXactCutoff = OldHeap->rd_rel->relminmxid;

	/* return selected values to caller */
	*pFreezeXid = FreezeXid;
	*pCutoffMulti = MultiXactCutoff;

	if(IsSystemRelation(OldHeap))
		elog(ERROR, "Cannot recluster a system relation.");

	/* Initialize the rewrite operation */
	rwstate = begin_heap_rewrite(OldHeap, NewHeap, OldestXmin, FreezeXid,
								 MultiXactCutoff, use_wal);

	/*
	 * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
	 * the OldHeap.  We know how to use a sort to duplicate the ordering of a
	 * btree index, and will use seqscan-and-sort for that case if the planner
	 * tells us it's cheaper.  Otherwise, always indexscan if an index is
	 * provided, else plain seqscan.
	 */
	if (OldIndex != NULL && OldIndex->rd_rel->relam == BTREE_AM_OID)
		use_sort = plan_cluster_use_sort(OIDOldHeap, OIDOldIndex);
	else
		use_sort = false;

	/* Set up sorting if wanted */
	if (use_sort)
		tuplesort = tuplesort_begin_cluster(oldTupDesc, OldIndex,
											maintenance_work_mem, false);
	else
		tuplesort = NULL;

	/*
	 * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
	 * that still need to be copied, we scan with SnapshotAny and use
	 * HeapTupleSatisfiesVacuum for the visibility test.
	 */
	if (OldIndex != NULL && !use_sort)
	{
		heapScan = NULL;
		indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, 0, 0);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		heapScan = heap_beginscan(OldHeap, SnapshotAny, 0, (ScanKey) NULL);
		indexScan = NULL;
	}

	/* Log what we're doing */
	if (indexScan != NULL)
		ereport(elevel,
				(errmsg("clustering \"%s.%s\" using index scan on \"%s\"",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap),
						RelationGetRelationName(OldIndex))));
	else if (tuplesort != NULL)
		ereport(elevel,
				(errmsg("clustering \"%s.%s\" using sequential scan and sort",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap))));
	else
		ereport(elevel,
				(errmsg("vacuuming \"%s.%s\"",
						get_namespace_name(RelationGetNamespace(OldHeap)),
						RelationGetRelationName(OldHeap))));

	/*
	 * Scan through the OldHeap, either in OldIndex order or sequentially;
	 * copy each tuple into the NewHeap, or transiently to the tuplesort
	 * module.  Note that we don't bother sorting dead tuples (they won't get
	 * to the new table anyway).
	 */
	for (;;)
	{
		HeapTuple	tuple;
		Buffer		buf;
		bool		isdead;

		CHECK_FOR_INTERRUPTS();

		if (indexScan != NULL)
		{
			tuple = index_getnext(indexScan, ForwardScanDirection);
			if (tuple == NULL)
				break;

			/* Since we used no scan keys, should never need to recheck */
			if (indexScan->xs_recheck)
				elog(ERROR, "CLUSTER does not support lossy index conditions");

			buf = indexScan->xs_cbuf;
		}
		else
		{
			tuple = heap_getnext(heapScan, ForwardScanDirection);
			if (tuple == NULL)
				break;

			buf = heapScan->rs_cbuf;
		}

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case HEAPTUPLE_RECENTLY_DEAD:
				tups_recently_dead += 1;
				/* fall through */
			case HEAPTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
				break;
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Since we hold exclusive lock on the relation, normally the
				 * only way to see this is if it was inserted earlier in our
				 * own transaction.  However, it can happen in system
				 * catalogs, since we tend to release write lock before commit
				 * there.  Give a warning if neither case applies; but in any
				 * case we had better copy it.
				 */
				if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
					elog(WARNING, "concurrent insert in progress within table \"%s\"",
						 RelationGetRelationName(OldHeap));
				/* treat as live */
				isdead = false;
				break;
			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * Similar situation to INSERT_IN_PROGRESS case.
				 */
				if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple->t_data)))
					elog(WARNING, "concurrent delete in progress within table \"%s\"",
						 RelationGetRelationName(OldHeap));
				/* treat as recently dead */
				tups_recently_dead += 1;
				isdead = false;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (isdead)
		{
			tups_vacuumed += 1;
			/* heap rewrite module still needs to see it... */
			if (rewrite_heap_dead_tuple(rwstate, tuple))
			{
				/* A previous recently-dead tuple is now known dead */
				tups_vacuumed += 1;
				tups_recently_dead -= 1;
			}
			continue;
		}

		num_tuples += 1;
		if (tuplesort != NULL)
			tuplesort_putheaptuple(tuplesort, tuple);
		else
			reform_and_rewrite_tuple(tuple,
									 oldTupDesc, newTupDesc,
									 values, isnull,
									 NewHeap->rd_rel->relhasoids, rwstate);
	}

	if (indexScan != NULL)
		index_endscan(indexScan);
	if (heapScan != NULL)
		heap_endscan(heapScan);

	/*
	 * In scan-and-sort mode, complete the sort, then read out all live tuples
	 * from the tuplestore and write them to the new relation.
	 */
	if (tuplesort != NULL)
	{
		tuplesort_performsort(tuplesort);

		for (;;)
		{
			HeapTuple	tuple;

			CHECK_FOR_INTERRUPTS();

			tuple = tuplesort_getheaptuple(tuplesort, true);
			if (tuple == NULL)
				break;

			reform_and_rewrite_tuple(tuple,
									 oldTupDesc, newTupDesc,
									 values, isnull,
									 NewHeap->rd_rel->relhasoids, rwstate);
		}

		tuplesort_end(tuplesort);
	}

	/* Write out any remaining tuples, and fsync if needed */
	end_heap_rewrite(rwstate);

	/* Reset rd_toastoid just to be tidy --- it shouldn't be looked at again */
	NewHeap->rd_toastoid = InvalidOid;

	num_pages = RelationGetNumberOfBlocks(NewHeap);

	/* Log what we did */
	ereport(elevel,
			(errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
					RelationGetRelationName(OldHeap),
					tups_vacuumed, num_tuples,
					RelationGetNumberOfBlocks(OldHeap)),
			 errdetail("%.0f dead row versions cannot be removed yet.\n"
					   "%s.",
					   tups_recently_dead,
					   pg_rusage_show(&ru0))));

	/* Clean up */
	pfree(values);
	pfree(isnull);

	if (OldIndex != NULL)
		index_close(OldIndex, NoLock);
	heap_close(OldHeap, NoLock);
	heap_close(NewHeap, NoLock);

	/* Update pg_class to reflect the correct values of pages and tuples. */
	relRelation = heap_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDNewHeap));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", OIDNewHeap);
	relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = num_pages;
	relform->reltuples = num_tuples;

	/* Don't update the stats for pg_class.  See swap_relation_files. */
	if (OIDOldHeap != RelationRelationId)
		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);
	else
		CacheInvalidateRelcacheByTuple(reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	heap_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();
}



/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 *
 * NB: new_index_oids must be in the same order as RelationGetIndexList
 *
 */
static void
finish_heap_swaps(Oid OIDOldHeap, Oid OIDNewHeap,
				 List		*new_index_oids,
				 bool swap_toast_by_content,
				 bool is_internal,
				 TransactionId frozenXid,
				 MultiXactId cutoffMulti)
{
	ObjectAddress object;
	Relation	oldHeapRel;
	List 		*old_index_oids;
	ListCell 	*old_index_cell;
	ListCell 	*new_index_cell;

	//TODO set deadlock time out to some large number
	oldHeapRel = heap_open(OIDOldHeap, AccessExclusiveLock);

	/*
	 * Swap the contents of the heap relations (including any toast tables).
	 * Also set old heap's relfrozenxid to frozenXid.
	 */
	swap_relation_files(OIDOldHeap, OIDNewHeap,
						swap_toast_by_content, is_internal,
						frozenXid, cutoffMulti);

	/* Swap the contents of the indexes */
	old_index_oids = RelationGetIndexList(oldHeapRel);
	forboth(old_index_cell, old_index_oids, new_index_cell, new_index_oids)
	{
		Oid			old_index_oid = lfirst_oid(old_index_cell);
		Oid			new_index_oid = lfirst_oid(new_index_cell);

		swap_relation_files(old_index_oid, new_index_oid,
						//TODO
						swap_toast_by_content, true,
						frozenXid, cutoffMulti);
	}
	//TODO assert same length?
	heap_close(oldHeapRel, NoLock);

	/* Destroy new heap with old filenode */
	object.classId = RelationRelationId;
	object.objectId = OIDNewHeap;
	object.objectSubId = 0;

	/*
	 * The new relation is local to our transaction and we know nothing
	 * depends on it, so DROP_RESTRICT should be OK.
	 */
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* performDeletion does CommandCounterIncrement at end */

	/*
	 * At this point, everything is kosher except that, if we did toast swap
	 * by links, the toast table's name corresponds to the transient table.
	 * The name is irrelevant to the backend because it's referenced by OID,
	 * but users looking at the catalogs could be confused.  Rename it to
	 * prevent this problem.
	 *
	 * Note no lock required on the relation, because we already hold an
	 * exclusive lock on it.
	 */
	if (!swap_toast_by_content)
	{
		Relation	newrel;

		newrel = heap_open(OIDOldHeap, NoLock);
		if (OidIsValid(newrel->rd_rel->reltoastrelid))
		{
			Oid			toastidx;
			char		NewToastName[NAMEDATALEN];

			/* Get the associated valid index to be renamed */
			toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid,
											 AccessShareLock);

			/* rename the toast table ... */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u",
					 OIDOldHeap);
			RenameRelationInternal(newrel->rd_rel->reltoastrelid,
								   NewToastName, true);

			/* ... and its valid index too. */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index",
					 OIDOldHeap);

			RenameRelationInternal(toastidx,
								   NewToastName, true);
		}
		relation_close(newrel, NoLock);
	}
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenode) while keeping the
 * same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
static void
swap_relation_files(Oid r1, Oid r2,
					bool swap_toast_by_content,
					bool is_internal,
					TransactionId frozenXid,
					MultiXactId cutoffMulti)
{
	Relation	relRelation;
	HeapTuple	reltup1,
				reltup2;
	Form_pg_class relform1,
				relform2;
	Oid			relfilenode1,
				relfilenode2;
	Oid			swaptemp;
	char		swptmpchr;

	/* We need writable copies of both pg_class tuples. */
	relRelation = heap_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenode1 = relform1->relfilenode;
	relfilenode2 = relform2->relfilenode;

	if (!OidIsValid(relfilenode1) || !OidIsValid(relfilenode2))
		elog(ERROR, "cannot recluster mapped relation \"%s\".",
				 NameStr(relform1->relname));

	/* swap relfilenodes, reltablespaces, relpersistence */

	swaptemp = relform1->relfilenode;
	relform1->relfilenode = relform2->relfilenode;
	relform2->relfilenode = swaptemp;

	swaptemp = relform1->reltablespace;
	relform1->reltablespace = relform2->reltablespace;
	relform2->reltablespace = swaptemp;

	swptmpchr = relform1->relpersistence;
	relform1->relpersistence = relform2->relpersistence;
	relform2->relpersistence = swptmpchr;

	/* Also swap toast links, if we're swapping by links */
	if (!swap_toast_by_content)
	{
		swaptemp = relform1->reltoastrelid;
		relform1->reltoastrelid = relform2->reltoastrelid;
		relform2->reltoastrelid = swaptemp;
	}

	/*
	 * In the case of a shared catalog, these next few steps will only affect
	 * our own database's pg_class row; but that's okay, because they are all
	 * noncritical updates.  That's also an important fact for the case of a
	 * mapped catalog, because it's possible that we'll commit the map change
	 * and then fail to commit the pg_class update.
	 */

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		Assert(TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;
		Assert(MultiXactIdIsValid(cutoffMulti));
		relform1->relminmxid = cutoffMulti;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32		swap_pages;
		float4		swap_tuples;
		int32		swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	/* Update the tuples in pg_class. */
	{
		CatalogIndexState indstate;
		indstate = CatalogOpenIndexes(relRelation);
		CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1,
									indstate);
		CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2,
									indstate);
		CatalogCloseIndexes(indstate);
	}

	/*
	 * Post alter hook for modified relations. The change to r2 is always
	 * internal, but r1 depends on the invocation context.
	 */
	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0,
								 InvalidOid, is_internal);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0,
								 InvalidOid, true);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * deal with them too.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		if (swap_toast_by_content)
		{
			if (relform1->reltoastrelid && relform2->reltoastrelid)
			{
				/* Recursively swap the contents of the toast tables */
				swap_relation_files(relform1->reltoastrelid,
									relform2->reltoastrelid,
									swap_toast_by_content,
									is_internal,
									frozenXid,
									cutoffMulti);
			}
			else
			{
				/* caller messed up */
				elog(ERROR, "cannot swap toast files by content when there's only one");
			}
		}
		else
		{
			/*
			 * We swapped the ownership links, so we need to change dependency
			 * data to match.
			 *
			 * NOTE: it is possible that only one table has a toast table.
			 *
			 * NOTE: at present, a TOAST table's only dependency is the one on
			 * its owning table.  If more are ever created, we'd need to use
			 * something more selective than deleteDependencyRecordsFor() to
			 * get rid of just the link we want.
			 */
			ObjectAddress baseobject,
						toastobject;
			long		count;

			/*
			 * We disallow this case for system catalogs, to avoid the
			 * possibility that the catalog we're rebuilding is one of the
			 * ones the dependency changes would change.  It's too late to be
			 * making any data changes to the target catalog.
			 */
			if (IsSystemClass(r1, relform1))
				elog(ERROR, "cannot swap toast files by links for system catalogs");

			/* Delete old dependencies */
			if (relform1->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform1->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}
			if (relform2->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform2->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}

			/* Register new dependencies */
			baseobject.classId = RelationRelationId;
			baseobject.objectSubId = 0;
			toastobject.classId = RelationRelationId;
			toastobject.objectSubId = 0;

			if (relform1->reltoastrelid)
			{
				baseobject.objectId = r1;
				toastobject.objectId = relform1->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}

			if (relform2->reltoastrelid)
			{
				baseobject.objectId = r2;
				toastobject.objectId = relform2->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}
		}
	}

	/*
	 * If we're swapping two toast tables by content, do the same for their
	 * valid index. The swap can actually be safely done only if the relations
	 * have indexes.
	 */
	if (swap_toast_by_content &&
		relform1->relkind == RELKIND_TOASTVALUE &&
		relform2->relkind == RELKIND_TOASTVALUE)
	{
		Oid			toastIndex1,
					toastIndex2;

		/* Get valid index for each relation */
		toastIndex1 = toast_get_valid_index(r1,
											AccessExclusiveLock);
		toastIndex2 = toast_get_valid_index(r2,
											AccessExclusiveLock);

		swap_relation_files(toastIndex1,
							toastIndex2,
							swap_toast_by_content,
							is_internal,
							InvalidTransactionId,
							InvalidMultiXactId);
	}

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	heap_close(relRelation, RowExclusiveLock);

	/*
	 * Close both relcache entries' smgr links.  We need this kluge because
	 * both links will be invalidated during upcoming CommandCounterIncrement.
	 * Whichever of the rels is the second to be cleared will have a dangling
	 * reference to the other's smgr entry.  Rather than trying to avoid this
	 * by ordering operations just so, it's easiest to close the links first.
	 * (Fortunately, since one of the entries is local in our transaction,
	 * it's sufficient to clear out our own relcache this way; the problem
	 * cannot arise for other backends when they see our update on the
	 * non-transient relation.)
	 *
	 * Caution: the placement of this step interacts with the decision to
	 * handle toast rels by recursion.  When we are trying to rebuild pg_class
	 * itself, the smgr close on pg_class must happen after all accesses in
	 * this function.
	 */
	RelationCloseSmgrByOid(r1);
	RelationCloseSmgrByOid(r2);
}


/*
 * Reconstruct and rewrite the given tuple
 *
 * We cannot simply copy the tuple as-is, for several reasons:
 *
 * 1. We'd like to squeeze out the values of any dropped columns, both
 * to save space and to ensure we have no corner-case failures. (It's
 * possible for example that the new table hasn't got a TOAST table
 * and so is unable to store any large values of dropped cols.)
 *
 * 2. The tuple might not even be legal for the new table; this is
 * currently only known to happen as an after-effect of ALTER TABLE
 * SET WITHOUT OIDS.
 *
 * So, we must reconstruct the tuple from component Datums.
 */
static void
reform_and_rewrite_tuple(HeapTuple tuple,
						 TupleDesc oldTupDesc, TupleDesc newTupDesc,
						 Datum *values, bool *isnull,
						 bool newRelHasOids, RewriteState rwstate)
{
	HeapTuple	copiedTuple;
	int			i;

	heap_deform_tuple(tuple, oldTupDesc, values, isnull);

	/* Be sure to null out any dropped columns */
	for (i = 0; i < newTupDesc->natts; i++)
	{
		if (newTupDesc->attrs[i]->attisdropped)
			isnull[i] = true;
	}

	copiedTuple = heap_form_tuple(newTupDesc, values, isnull);

	/* Preserve OID, if any */
	if (newRelHasOids)
		HeapTupleSetOid(copiedTuple, HeapTupleGetOid(tuple));

	/* The heap rewrite module does the rest */
	rewrite_heap_tuple(rwstate, tuple, copiedTuple);

	heap_freetuple(copiedTuple);
}
