DO $$
DECLARE
	nsp TEXT;
BEGIN
	FOR nsp IN
		SELECT nspname
		FROM (
			SELECT nspname
			FROM gp_dist_random('pg_namespace')
			WHERE nspname ~ '^pg(_toast)?_temp_[0-9]+'
			UNION
			SELECT nspname
			FROM pg_namespace
			WHERE nspname ~ '^pg(_toast)?_temp_[0-9]+'
		) n
		LEFT OUTER JOIN pg_stat_activity x on
			sess_id = regexp_replace(nspname, 'pg(_toast)?_temp_', '')::int
		WHERE x.sess_id is null
	LOOP
		EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', nsp);
	END LOOP;
END $$;
