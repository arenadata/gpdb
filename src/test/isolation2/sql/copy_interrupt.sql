!\retcode gpconfig -c client_connection_check_interval -v 20s;
!\retcode gpstop -u;

0: CREATE TABLE t(a int);
0&: COPY t FROM PROGRAM 'while true; do echo 1; sleep 1; done | cat -';

0t:
! sleep 40;

SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity where query LIKE 'COPY t FROM PROGRAM%';

DROP TABLE t;
