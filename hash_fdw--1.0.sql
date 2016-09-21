\echo Use "create extension hash_fdw" to load this file. \quit

--CREATE TYPE testvalue;
CREATE TYPE testvalue as(name text,supplier_id integer,price integer,mid_name text);

CREATE OR REPLACE FUNCTION hash_insert_hashdata(int, cstring, record) RETURNS int
	AS 'MODULE_PATHNAME', 'hash_insert_hashdata'
	LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION hash_get_hashdata(int, cstring) RETURNS record
	AS 'MODULE_PATHNAME', 'hash_get_hashdata'
	LANGUAGE C strict;

CREATE FUNCTION hash_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION hash_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hash_fdw
  HANDLER hash_fdw_handler
  VALIDATOR hash_fdw_validator;
