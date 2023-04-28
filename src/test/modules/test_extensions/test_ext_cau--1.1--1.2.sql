/* src/test/modules/test_extensions/test_ext_cau--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION test_ext_cau" to load this file. \quit

DROP function test_func2(int, int);
