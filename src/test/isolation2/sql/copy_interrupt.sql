!\retcode gpconfig -c client_connection_check_interval -v 20s;
!\retcode gpstop -u;

0: CREATE TABLE copy_interrupt_table(a int);
0&: COPY copy_interrupt_table FROM PROGRAM 'while true; do echo 1; sleep 1; done | cat -';
! sleep 10;

0t:
! sleep 40;

SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity where query LIKE 'COPY copy_interrupt_table FROM PROGRAM%';

DROP TABLE copy_interrupt_table;
!\retcode gpconfig -c client_connection_check_interval -v 0;