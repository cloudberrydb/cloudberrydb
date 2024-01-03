#include "storage/paxc_block_map_manager.h"

#include <unistd.h>

#include "comm/cbdb_wrappers.h"

#ifndef ENABLE_LOCAL_INDEX
namespace paxc {
static PaxXactSharedState *current_xact_shared_pax_ss = NULL;

#define DEFAULT_BLOCK_IDS_SIZE 16

#define ACQUIRE_HASH_LOCK(hash_lock, lockmode) \
  LWLockAcquire(&hash_lock->lock, lockmode);

#define RELEASE_HASH_LOCK(hash_lock) LWLockRelease(&hash_lock->lock)

static LocalTableBlockMappingData
    local_pax_block_mapping_data[BLOCK_MAPPING_ARRAY_SIZE];

static MemoryContext pax_block_mapping_context = NULL;

static int max_procs = 0;
// the lock for pax_xact_hash
static LWLockPadded *pax_hash_lock = NULL;
static HTAB *pax_xact_hash = NULL;
// common PaxSharedState
static PaxSharedState *pax_shared_state;

static Size struct_mem_size() { return sizeof(PaxSharedState); }

static Size pax_mem_size() {
  Size size = 0;
  size = add_size(size, struct_mem_size());
  // hash size
  size = add_size(size, hash_estimate_size(max_procs, sizeof(XactHashEntry)));
  return size;
}

static void init_pax_xact_hash() {
  HASHCTL info;
  memset(&info, 0, sizeof(HASHCTL));
  info.keysize = sizeof(XactHashKey);
  info.entrysize = sizeof(XactHashEntry);
  pax_xact_hash = ShmemInitHash("pax_xact_hash", max_procs, max_procs, &info,
                                HASH_ELEM | HASH_BLOBS);
}

static void init_shmem_locks() {
  pax_hash_lock = GetNamedLWLockTranche("pax_hash_lock");
}

void paxc_shmem_request() {
  max_procs = MaxConnections;
  RequestAddinShmemSpace(pax_mem_size());
  RequestNamedLWLockTranche("pax_hash_lock", 1);
}

void paxc_shmem_startup() {
  bool found;
  current_xact_shared_pax_ss = NULL;
  pax_block_mapping_context = AllocSetContextCreate(
      TopMemoryContext, "Pax Block Mapping Context", ALLOCSET_DEFAULT_SIZES);

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  pax_shared_state = reinterpret_cast<PaxSharedState *>(
      ShmemInitStruct("pax_shared_stat", struct_mem_size(), &found));
  if (!found) {
    pax_shared_state->pax_xact_lock_tranche_id_ = LWLockNewTrancheId();
  }
  LWLockRegisterTranche(pax_shared_state->pax_xact_lock_tranche_id_,
                        "pax_xact_array_locks");
  init_pax_xact_hash();
  init_shmem_locks();
  LWLockRelease(AddinShmemInitLock);
  if (pax_shared_state == NULL) {
    ereport(
        FATAL,
        (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
         errdetail("Failed while allocation block %lu bytes in shared memory.",
                   struct_mem_size())));
  }
}

// there may be multiple scan processes scanning the same table,
// which will modify the current_xact_shared_pax_ss state,
// so a LW_EXCLUSIVE lock is required
void get_table_index_and_table_number(Oid table_rel_oid, uint8 *table_no,
                                      uint32 *table_index) {
  LWLockAcquire(&current_xact_shared_pax_ss->lock_, LW_EXCLUSIVE);
  uint8 alloc_table_no = 0;
  for (uint32 i = 0; i < current_xact_shared_pax_ss->block_mapping_used_size_;
       i++) {
    if (current_xact_shared_pax_ss->shared_block_mapping_[i].relid_ ==
        table_rel_oid) {
      alloc_table_no++;
    }
  }
  *table_index = current_xact_shared_pax_ss->block_mapping_used_size_++;
  *table_no = alloc_table_no;
  ereport(
      DEBUG1,
      (errmsg("get_table_index_and_table_number pax_xact_hash=%p, lock=%p,"
              "gp_session_id=%d "
              "pid=%d, db_id=%d, segment_id=%d, gp_command_id=%d "
              "table_oid=%d, table_index=%d, "
              "table_no=%d, is_gp_writer=%d, shmem_ptr=%p, lock=%p",
              pax_xact_hash, &pax_hash_lock->lock, gp_session_id, getpid(),
              GpIdentity.dbid, GpIdentity.segindex, gp_command_count,
              table_rel_oid, *table_index, *table_no, Gp_is_writer,
              current_xact_shared_pax_ss, &current_xact_shared_pax_ss->lock_)));
  LWLockRelease(&current_xact_shared_pax_ss->lock_);
  if (*table_index >= BLOCK_MAPPING_ARRAY_SIZE) {
    ereport(ERROR,
            (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("out of array size"),
             errdetail("Failed while allocation table slot %d in  "
                       "current_xact_shared_pax_ss->shared_block_mapping_, max "
                       "size is %d",
                       *table_index, BLOCK_MAPPING_ARRAY_SIZE)));
  }
  if (alloc_table_no > MAX_TABLE_NUM_IN_CTID) {
    ereport(ERROR,
            (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("out of array size"),
             errdetail("Failed while table no %d overflow the max table num %d",
                       *table_no, MAX_TABLE_NUM_IN_CTID)));
  }
}

// FIXME(gongxun): the delete and update processes only read
// current_xact_shared_pax_ss , whether it is possible to not add a shared lock?
static uint32 pax_get_table_index(Oid table_rel_oid, uint8 table_no) {
  LWLockAcquire(&current_xact_shared_pax_ss->lock_, LW_SHARED);
  uint8 tmp_table_no = -1;
  int index = -1;
  for (uint32 i = 0; i < current_xact_shared_pax_ss->block_mapping_used_size_;
       i++) {
    if (current_xact_shared_pax_ss->shared_block_mapping_[i].relid_ ==
        table_rel_oid) {
      if ((++tmp_table_no) == table_no) {
        index = i;
        break;
      }
    }
  }
  LWLockRelease(&current_xact_shared_pax_ss->lock_);
  Assert(tmp_table_no == table_no);
  return index;
}

static void dump_shared_block_ids(Oid table_rel_oid, uint32 table_index) {
  LocalTableBlockMappingData *block_mapping_data =
      &local_pax_block_mapping_data[table_index];

  PaxBlockId *shared_ptr;

  // save old segment, if shared memory is full, we need to alloc a new
  // segment and copy old data,then free old segment
  dsm_segment *old_segment = block_mapping_data->block_ids_segment_;
  dsm_segment *new_segment = nullptr;
  block_mapping_data->relid_ = table_rel_oid;

  // if local memory size is large than shared memory's size, we need to
  // resize shared memory
  SharedTableBlockMappingData *shared_block_mapping_data =
      &current_xact_shared_pax_ss->shared_block_mapping_[table_index];
  ereport(DEBUG1, (errmsg("dump_shared_block_ids pax_xact_hash=%p, lock=%p,"
                          "gp_session_id=%d "
                          "pid=%d, db_id=%d, segment_id=%d, gp_command_id=%d "
                          "table_oid=%d, table_index=%d, local_size =%d, "
                          "shared_size=%d, is_gp_writer=%d",
                          pax_xact_hash, &pax_hash_lock->lock, gp_session_id,
                          getpid(), GpIdentity.dbid, GpIdentity.segindex,
                          gp_command_count, table_rel_oid, table_index,
                          block_mapping_data->used_block_ids_,
                          shared_block_mapping_data->shared_size_block_ids_,
                          Gp_is_writer)));
  if (block_mapping_data->used_block_ids_ >
          shared_block_mapping_data->shared_size_block_ids_ ||
      old_segment == NULL) {
    // need to resize shared memory
    ResourceOwner oldowner;
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = TopTransactionResourceOwner;

    uint32 new_size = block_mapping_data->size_block_ids_;
    new_segment = dsm_create(sizeof(PaxBlockId) * new_size,
                             DSM_CREATE_NULL_IF_MAXSEGMENTS);

    if (new_segment == NULL) {
      ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                      errmsg("could not create DSM segment for %d PaxBlockId",
                             new_size)));
    }

    dsm_pin_mapping(new_segment);
    dsm_pin_segment(new_segment);
    block_mapping_data->block_ids_segment_ = new_segment;

    shared_block_mapping_data->shared_size_block_ids_ = new_size;

    shared_block_mapping_data->shared_used_block_ids_ = 0;

    shared_ptr =
        reinterpret_cast<PaxBlockId *>(dsm_segment_address(new_segment));

    CurrentResourceOwner = oldowner;
  }

  shared_ptr = reinterpret_cast<PaxBlockId *>(
      dsm_segment_address(block_mapping_data->block_ids_segment_));

  memcpy(
      shared_ptr + shared_block_mapping_data->shared_used_block_ids_,
      block_mapping_data->block_ids_ +
          shared_block_mapping_data->shared_used_block_ids_,
      sizeof(PaxBlockId) * (block_mapping_data->used_block_ids_ -
                            shared_block_mapping_data->shared_used_block_ids_));

  pg_write_barrier();
  shared_block_mapping_data->shared_used_block_ids_ =
      block_mapping_data->used_block_ids_;

  shared_block_mapping_data->relid_ = block_mapping_data->relid_;
  if (old_segment != block_mapping_data->block_ids_segment_) {
    shared_block_mapping_data->shared_block_ids_handle_ =
        dsm_segment_handle(block_mapping_data->block_ids_segment_);

    if (old_segment) {
      dsm_unpin_segment(dsm_segment_handle(old_segment));
      dsm_detach(old_segment);
    }
  }
}

static void load_shared_block_ids(Oid table_rel_oid, uint32 table_index) {
  dsm_handle table_block_mapping_handle;
  dsm_segment *attached_block_ids;
  PaxBlockId *shared_ptr;

  for (;;) {
    table_block_mapping_handle =
        current_xact_shared_pax_ss->shared_block_mapping_[table_index]
            .shared_block_ids_handle_;

    attached_block_ids = dsm_attach(table_block_mapping_handle);
    if (attached_block_ids != NULL) break;

    if (table_block_mapping_handle ==
        current_xact_shared_pax_ss->shared_block_mapping_[table_index]
            .shared_block_ids_handle_) {
      elog(ERROR, "could not attach to table(%d) shared block ids array",
           table_rel_oid);
    }
  }

  shared_ptr =
      reinterpret_cast<PaxBlockId *>(dsm_segment_address(attached_block_ids));

  pg_read_barrier();

  if (current_xact_shared_pax_ss->shared_block_mapping_[table_index]
          .shared_used_block_ids_ >
      local_pax_block_mapping_data[table_index].size_block_ids_) {
    // resize local memory
    uint32 new_size =
        current_xact_shared_pax_ss->shared_block_mapping_[table_index]
            .shared_size_block_ids_;
    MemoryContext oldcontext = MemoryContextSwitchTo(pax_block_mapping_context);
    if (local_pax_block_mapping_data[table_index].block_ids_ == nullptr) {
      local_pax_block_mapping_data[table_index].used_block_ids_ = 0;
      local_pax_block_mapping_data[table_index].size_block_ids_ = new_size;
      local_pax_block_mapping_data[table_index].block_ids_ =
          reinterpret_cast<PaxBlockId *>(
              palloc0(sizeof(PaxBlockId) * new_size));
    } else {
      local_pax_block_mapping_data[table_index].block_ids_ =
          reinterpret_cast<PaxBlockId *>(
              repalloc(local_pax_block_mapping_data[table_index].block_ids_,
                       sizeof(PaxBlockId) * new_size));
    }
    MemoryContextSwitchTo(oldcontext);
    local_pax_block_mapping_data[table_index].size_block_ids_ = new_size;
  }

  memcpy(local_pax_block_mapping_data[table_index].block_ids_ +
             local_pax_block_mapping_data[table_index].used_block_ids_,
         shared_ptr + local_pax_block_mapping_data[table_index].used_block_ids_,
         (current_xact_shared_pax_ss->shared_block_mapping_[table_index]
              .shared_used_block_ids_ -
          local_pax_block_mapping_data[table_index].used_block_ids_) *
             sizeof(PaxBlockId));
  local_pax_block_mapping_data[table_index].relid_ = table_rel_oid;
  local_pax_block_mapping_data[table_index].used_block_ids_ =
      current_xact_shared_pax_ss->shared_block_mapping_[table_index]
          .shared_used_block_ids_;

  dsm_detach(attached_block_ids);
}

uint32 get_block_number(Oid table_rel_oid, uint32 table_index,
                        const PaxBlockId &block_id) {
  LocalTableBlockMappingData *block_mapping_data =
      &local_pax_block_mapping_data[table_index];

  if (block_mapping_data->used_block_ids_ >=
      block_mapping_data->size_block_ids_) {
    MemoryContext oldcontext = MemoryContextSwitchTo(pax_block_mapping_context);
    if (block_mapping_data->block_ids_ == nullptr) {
      block_mapping_data->block_ids_ = reinterpret_cast<PaxBlockId *>(
          palloc0(sizeof(PaxBlockId) * DEFAULT_BLOCK_IDS_SIZE));
      block_mapping_data->size_block_ids_ = DEFAULT_BLOCK_IDS_SIZE;
      block_mapping_data->block_ids_segment_ = NULL;
    } else {
      uint32 new_size = block_mapping_data->size_block_ids_ * 2;
      block_mapping_data->block_ids_ = reinterpret_cast<PaxBlockId *>(repalloc(
          block_mapping_data->block_ids_, sizeof(PaxBlockId) * new_size));
      block_mapping_data->size_block_ids_ = new_size;
    }
    MemoryContextSwitchTo(oldcontext);
  }

  uint32 block_number = block_mapping_data->used_block_ids_++;
  block_mapping_data->block_ids_[block_number] = block_id;
  // TODO(gongxun): should we add the condition that only Gp_reader dump
  // to shared memory?
  dump_shared_block_ids(table_rel_oid, table_index);
  return block_number;
}

static PaxBlockId pax_get_block_id(Oid table_rel_oid, uint32 table_index,
                                   uint32 block_number) {
  LocalTableBlockMappingData *block_mapping_data =
      &local_pax_block_mapping_data[table_index];

  if (block_mapping_data->relid_ != table_rel_oid ||
      block_number >= block_mapping_data->used_block_ids_) {
    load_shared_block_ids(table_rel_oid, table_index);
  }

  block_mapping_data = &local_pax_block_mapping_data[table_index];
  if (block_number >= block_mapping_data->used_block_ids_) {
    elog(FATAL, "invalid block number %d for table %d", block_number,
         table_rel_oid);
  }
  return block_mapping_data->block_ids_[block_number];
}

PaxBlockId get_block_id(Oid table_rel_oid, uint8 table_no,
                        uint32 block_number) {
  uint32 table_index = pax_get_table_index(table_rel_oid, table_no);
  return pax_get_block_id(table_rel_oid, table_index, block_number);
}

static void init_command_shmem_resource() {
  XactHashKey pax_xact_hash_key;
  pax_xact_hash_key.session_id_ = gp_session_id;
  pax_xact_hash_key.command_id_ = gp_command_count;
  XactHashEntry *entry = NULL;
  bool found;
  ACQUIRE_HASH_LOCK(pax_hash_lock, LW_EXCLUSIVE);
  entry = reinterpret_cast<XactHashEntry *>(
      hash_search(pax_xact_hash, &pax_xact_hash_key, HASH_ENTER, &found));
  if (!found) {
    entry->key_ = pax_xact_hash_key;
    LWLockInitialize(&entry->shared_state_.lock_,
                     pax_shared_state->pax_xact_lock_tranche_id_);
    entry->shared_state_.block_mapping_used_size_ = 0;
  } else {
    if (!entry) {
      ereport(FATAL,
              (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
               errdetail(
                   "Failed while allocation block %lu bytes in shared memory.",
                   sizeof(XactHashEntry))));
    }
  }

  ereport(DEBUG1,
          (errmsg("init_command_shmem_resource pax_xact_hash=%p, lock=%p,"
                  "gp_session_id=%d "
                  "pid=%d, db_id=%d, segment_id=%d, found=%s, gp_command_id=%d "
                  ",gp_is_writer=%d",
                  pax_xact_hash, &pax_hash_lock->lock, gp_session_id, getpid(),
                  GpIdentity.dbid, GpIdentity.segindex,
                  found ? "true" : "false", gp_command_count, Gp_is_writer)));
  RELEASE_HASH_LOCK(pax_hash_lock);
  current_xact_shared_pax_ss = &entry->shared_state_;
}

// FIXME(gongxun): do we need to lock current_xact_shared_pax_ss here to protect
// the case where gp_reader is still executing dump_shared_block_ids
static void cleanup_command_shmem_resource() {
  if (Gp_is_writer && current_xact_shared_pax_ss != NULL) {
    XactHashKey pax_xact_hash_key;
    pax_xact_hash_key.session_id_ = gp_session_id;
    pax_xact_hash_key.command_id_ = gp_command_count;
    bool found;
    ereport(DEBUG1,
            (errmsg("cleanup_command_shmem_resource pax_xact_hash=%p, "
                    "gp_session_id=%d "
                    "pid=%d, db_id=%d, segment_id=%d,  gp_command_id=%d "
                    ",gp_is_writer=%d",
                    pax_xact_hash, gp_session_id, getpid(), GpIdentity.dbid,
                    GpIdentity.segindex, gp_command_count, Gp_is_writer)));
    for (uint32 i = 0; i < current_xact_shared_pax_ss->block_mapping_used_size_;
         i++) {
      current_xact_shared_pax_ss->shared_block_mapping_[i].relid_ = InvalidOid;
      current_xact_shared_pax_ss->shared_block_mapping_[i]
          .shared_used_block_ids_ = 0;
      current_xact_shared_pax_ss->shared_block_mapping_[i]
          .shared_size_block_ids_ = 0;
      if (current_xact_shared_pax_ss->shared_block_mapping_[i]
              .shared_block_ids_handle_ != 0) {
        dsm_unpin_segment(current_xact_shared_pax_ss->shared_block_mapping_[i]
                              .shared_block_ids_handle_);
      }
      current_xact_shared_pax_ss->shared_block_mapping_[i]
          .shared_block_ids_handle_ = 0;
    }
    current_xact_shared_pax_ss->block_mapping_used_size_ = 0;
    ACQUIRE_HASH_LOCK(pax_hash_lock, LW_EXCLUSIVE);
    hash_search(pax_xact_hash, &pax_xact_hash_key, HASH_REMOVE, &found);
    RELEASE_HASH_LOCK(pax_hash_lock);
    current_xact_shared_pax_ss = NULL;
  }
}

static void init_local_command_resource() {
  for (uint32 i = 0; i < BLOCK_MAPPING_ARRAY_SIZE; i++) {
    local_pax_block_mapping_data[i].relid_ = InvalidOid;
    local_pax_block_mapping_data[i].size_block_ids_ = 0;
    local_pax_block_mapping_data[i].used_block_ids_ = 0;

    // FIXME(gongxun): unpin the segment and clean the local buffer, because if
    // transaction abort, we don't release the resource correctly, so we need to
    // unpin the segment here until we fix the bug
    local_pax_block_mapping_data[i].block_ids_ = NULL;
    if (local_pax_block_mapping_data[i].block_ids_segment_) {
      dsm_detach(local_pax_block_mapping_data[i].block_ids_segment_);
    }
    local_pax_block_mapping_data[i].block_ids_segment_ = NULL;
    if (local_pax_block_mapping_data[i].block_ids_) {
      pfree(local_pax_block_mapping_data[i].block_ids_);
    }
  }
}

static void cleanup_local_command_resource() {
  // TODO(gongxun): should only clean the slot used
  for (uint32 i = 0; i < BLOCK_MAPPING_ARRAY_SIZE; i++) {
    local_pax_block_mapping_data[i].relid_ = InvalidOid;
    local_pax_block_mapping_data[i].size_block_ids_ = 0;
    local_pax_block_mapping_data[i].used_block_ids_ = 0;
    if (local_pax_block_mapping_data[i].block_ids_) {
      pfree(local_pax_block_mapping_data[i].block_ids_);
      local_pax_block_mapping_data[i].block_ids_ = NULL;
    }
    if (local_pax_block_mapping_data[i].block_ids_segment_) {
      dsm_detach(local_pax_block_mapping_data[i].block_ids_segment_);
    }
    local_pax_block_mapping_data[i].block_ids_segment_ = NULL;
  }
  MemoryContextReset(pax_block_mapping_context);
}

void init_command_resource() {
  init_command_shmem_resource();
  init_local_command_resource();
}

void release_command_resource() {
  cleanup_local_command_resource();
  cleanup_command_shmem_resource();
}

}  // namespace paxc

namespace cbdb {
void InitCommandResource() {
  CBDB_WRAP_START;
  { paxc::init_command_resource(); }
  CBDB_WRAP_END;
}

void ReleaseCommandResource() {
  CBDB_WRAP_START;
  { paxc::release_command_resource(); }
  CBDB_WRAP_END;
}
}  // namespace cbdb

#endif
