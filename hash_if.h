#ifndef HASH_IF_H
#define HASH_IF_H

#define HASH_TABLE_COUNT 15

HTAB* hash_table_count[HASH_TABLE_COUNT];

typedef uint64 hash_count_t;

typedef struct hash_value_t
{
    HeapTupleHeader t_data;
} hash_value_t;

typedef struct {
    char* id;
} hash_key_t;

typedef struct {
    hash_key_t key;
    hash_value_t value;
} hash_entry_t;


#define MB (1024 * 1024)

hash_value_t* hash_insert_data(int hash_index, char const* key_id, HeapTupleHeader tuplehead);
hash_value_t* hash_get_data(int hash_index, char const* key_id);
void hash_del_data(int hash_index,char *key_id);
void hash_put_shmhandle(int hash_index, char const* key_id, void *data);
void *hash_get_shmhandle(int hash_index, char const* key_id);
extern int hash_idx;

#endif   /* HASH_IF_H */

