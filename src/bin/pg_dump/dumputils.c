/*-------------------------------------------------------------------------
 *
 * Utility routines for SQL dumping
 *	Basically this is stuff that is useful in both pg_dump and pg_dumpall.
 *	Lately it's also being used by psql and bin/scripts/ ...
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumputils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <ctype.h>

#include "dumputils.h"

#include "parser/keywords.h"


/* Globals from keywords.c */
extern const ScanKeyword FEScanKeywords[];
extern const int NumFEScanKeywords;

static bool parseAclItem(const char *item, const char *type,
			 const char *name, const char *subname, int remoteVersion,
			 PQExpBuffer grantee, PQExpBuffer grantor,
			 PQExpBuffer privs, PQExpBuffer privswgo);
static char *copyAclUserName(PQExpBuffer output, char *input);
static void AddAcl(PQExpBuffer aclbuf, const char *keyword,
	   const char *subname);
static PQExpBuffer defaultGetLocalPQExpBuffer(void);

/* Globals exported by this file */
int			quote_all_identifiers = 0;
PQExpBuffer (*getLocalPQExpBuffer) (void) = defaultGetLocalPQExpBuffer;

/*
 * Returns a temporary PQExpBuffer, valid until the next call to the function.
 * This is used by fmtId and fmtQualifiedId.
 *
 * Non-reentrant and non-thread-safe but reduces memory leakage. You can
 * replace this with a custom version by setting the getLocalPQExpBuffer
 * function pointer.
 */
static PQExpBuffer
defaultGetLocalPQExpBuffer(void)
{
	static PQExpBuffer id_return = NULL;

	if (id_return)				/* first time through? */
	{
		/* same buffer, just wipe contents */
		resetPQExpBuffer(id_return);
	}
	else
	{
		/* new buffer */
		id_return = createPQExpBuffer();
	}

	return id_return;
}

/*
 *	Quotes input string if it's not a legitimate SQL identifier as-is.
 *
 *	Note that the returned string must be used before calling fmtId again,
 *	since we re-use the same return buffer each time.
 */
const char *
fmtId(const char *rawid)
{
	PQExpBuffer id_return = getLocalPQExpBuffer();

	const char *cp;
	bool		need_quotes = false;

	/*
	 * These checks need to match the identifier production in scan.l. Don't
	 * use islower() etc.
	 */
	if (quote_all_identifiers)
		need_quotes = true;
	/* slightly different rules for first character */
	else if (!((rawid[0] >= 'a' && rawid[0] <= 'z') || rawid[0] == '_'))
		need_quotes = true;
	else
	{
		/* otherwise check the entire string */
		for (cp = rawid; *cp; cp++)
		{
			if (!((*cp >= 'a' && *cp <= 'z')
				  || (*cp >= '0' && *cp <= '9')
				  || (*cp == '_')))
			{
				need_quotes = true;
				break;
			}
		}
	}

	if (!need_quotes)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
		const ScanKeyword *keyword = ScanKeywordLookup(rawid,
													   FEScanKeywords,
													   NumFEScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			need_quotes = true;
	}

	if (!need_quotes)
	{
		/* no quoting needed */
		appendPQExpBufferStr(id_return, rawid);
	}
	else
	{
		appendPQExpBufferChar(id_return, '\"');
		for (cp = rawid; *cp; cp++)
		{
			/*
			 * Did we find a double-quote in the string? Then make this a
			 * double double-quote per SQL99. Before, we put in a
			 * backslash/double-quote pair. - thomas 2000-08-05
			 */
			if (*cp == '\"')
				appendPQExpBufferChar(id_return, '\"');
			appendPQExpBufferChar(id_return, *cp);
		}
		appendPQExpBufferChar(id_return, '\"');
	}

	return id_return->data;
}

/*
 * fmtQualifiedId - convert a qualified name to the proper format for
 * the source database.
 *
 * Like fmtId, use the result before calling again.
 *
 * Since we call fmtId and it also uses getThreadLocalPQExpBuffer() we cannot
 * use it until we're finished with calling fmtId().
 */
const char *
fmtQualifiedId(int remoteVersion, const char *schema, const char *id)
{
	PQExpBuffer id_return;
	PQExpBuffer lcl_pqexp = createPQExpBuffer();

	/* Some callers might fail to provide a schema name */
	if (schema && *schema)
	{
		appendPQExpBuffer(lcl_pqexp, "%s.", fmtId(schema));
	}
	appendPQExpBufferStr(lcl_pqexp, fmtId(id));

	id_return = getLocalPQExpBuffer();

	appendPQExpBufferStr(id_return, lcl_pqexp->data);
	destroyPQExpBuffer(lcl_pqexp);

	return id_return->data;
}

/*
 * Format a Postgres version number (in the PG_VERSION_NUM integer format
 * returned by PQserverVersion()) as a string.  This exists mainly to
 * encapsulate knowledge about two-part vs. three-part version numbers.
 *
 * For re-entrancy, caller must supply the buffer the string is put in.
 * Recommended size of the buffer is 32 bytes.
 *
 * Returns address of 'buf', as a notational convenience.
 */
char *
formatPGVersionNumber(int version_number, bool include_minor,
					  char *buf, size_t buflen)
{
	if (version_number >= 100000)
	{
		/* New two-part style */
		if (include_minor)
			snprintf(buf, buflen, "%d.%d", version_number / 10000,
					 version_number % 10000);
		else
			snprintf(buf, buflen, "%d", version_number / 10000);
	}
	else
	{
		/* Old three-part style */
		if (include_minor)
			snprintf(buf, buflen, "%d.%d.%d", version_number / 10000,
					 (version_number / 100) % 100,
					 version_number % 100);
		else
			snprintf(buf, buflen, "%d.%d", version_number / 10000,
					 (version_number / 100) % 100);
	}
	return buf;
}


/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  We assume the specified client_encoding and
 * standard_conforming_strings settings.
 *
 * This is essentially equivalent to libpq's PQescapeStringInternal,
 * except for the output buffer structure.  We need it in situations
 * where we do not have a PGconn available.  Where we do,
 * appendStringLiteralConn is a better choice.
 */
void
appendStringLiteral(PQExpBuffer buf, const char *str,
					int encoding, bool std_strings)
{
	size_t		length = strlen(str);
	const char *source = str;
	char	   *target;

	if (!enlargePQExpBuffer(buf, 2 * length + 2))
		return;

	target = buf->data + buf->len;
	*target++ = '\'';

	while (*source != '\0')
	{
		char		c = *source;
		int			len;
		int			i;

		/* Fast path for plain ASCII */
		if (!IS_HIGHBIT_SET(c))
		{
			/* Apply quoting if needed */
			if (SQL_STR_DOUBLE(c, !std_strings))
				*target++ = c;
			/* Copy the character */
			*target++ = c;
			source++;
			continue;
		}

		/* Slow path for possible multibyte characters */
		len = PQmblen(source, encoding);

		/* Copy the character */
		for (i = 0; i < len; i++)
		{
			if (*source == '\0')
				break;
			*target++ = *source++;
		}

		/*
		 * If we hit premature end of string (ie, incomplete multibyte
		 * character), try to pad out to the correct length with spaces. We
		 * may not be able to pad completely, but we will always be able to
		 * insert at least one pad space (since we'd not have quoted a
		 * multibyte character).  This should be enough to make a string that
		 * the server will error out on.
		 */
		if (i < len)
		{
			char	   *stop = buf->data + buf->maxlen - 2;

			for (; i < len; i++)
			{
				if (target >= stop)
					break;
				*target++ = ' ';
			}
			break;
		}
	}

	/* Write the terminating quote and NUL character. */
	*target++ = '\'';
	*target = '\0';

	buf->len = target - buf->data;
}


/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  Encoding and string syntax rules are as indicated
 * by current settings of the PGconn.
 */
void
appendStringLiteralConn(PQExpBuffer buf, const char *str, PGconn *conn)
{
	size_t		length = strlen(str);

	/*
	 * XXX This is a kluge to silence escape_string_warning in our utility
	 * programs.  It should go away someday.
	 */
	if (strchr(str, '\\') != NULL && PQserverVersion(conn) >= 80100)
	{
		/* ensure we are not adjacent to an identifier */
		if (buf->len > 0 && buf->data[buf->len - 1] != ' ')
			appendPQExpBufferChar(buf, ' ');
		appendPQExpBufferChar(buf, ESCAPE_STRING_SYNTAX);
		appendStringLiteral(buf, str, PQclientEncoding(conn), false);
		return;
	}
	/* XXX end kluge */

	if (!enlargePQExpBuffer(buf, 2 * length + 2))
		return;
	appendPQExpBufferChar(buf, '\'');
	buf->len += PQescapeStringConn(conn, buf->data + buf->len,
								   str, length, NULL);
	appendPQExpBufferChar(buf, '\'');
}


/*
 * Convert a string value to a dollar quoted literal and append it to
 * the given buffer. If the dqprefix parameter is not NULL then the
 * dollar quote delimiter will begin with that (after the opening $).
 *
 * No escaping is done at all on str, in compliance with the rules
 * for parsing dollar quoted strings.  Also, we need not worry about
 * encoding issues.
 */
void
appendStringLiteralDQ(PQExpBuffer buf, const char *str, const char *dqprefix)
{
	static const char suffixes[] = "_XXXXXXX";
	int			nextchar = 0;
	PQExpBuffer delimBuf = createPQExpBuffer();

	/* start with $ + dqprefix if not NULL */
	appendPQExpBufferChar(delimBuf, '$');
	if (dqprefix)
		appendPQExpBufferStr(delimBuf, dqprefix);

	/*
	 * Make sure we choose a delimiter which (without the trailing $) is not
	 * present in the string being quoted. We don't check with the trailing $
	 * because a string ending in $foo must not be quoted with $foo$.
	 */
	while (strstr(str, delimBuf->data) != NULL)
	{
		appendPQExpBufferChar(delimBuf, suffixes[nextchar++]);
		nextchar %= sizeof(suffixes) - 1;
	}

	/* add trailing $ */
	appendPQExpBufferChar(delimBuf, '$');

	/* quote it and we are all done */
	appendPQExpBufferStr(buf, delimBuf->data);
	appendPQExpBufferStr(buf, str);
	appendPQExpBufferStr(buf, delimBuf->data);

	destroyPQExpBuffer(delimBuf);
}


/*
 * Append the given string to the shell command being built in the buffer,
 * with suitable shell-style quoting to create exactly one argument.
 *
 * Forbid LF or CR characters, which have scant practical use beyond designing
 * security breaches.  The Windows command shell is unusable as a conduit for
 * arguments containing LF or CR characters.  A future major release should
 * reject those characters in CREATE ROLE and CREATE DATABASE, because use
 * there eventually leads to errors here.
 */
void
appendShellString(PQExpBuffer buf, const char *str)
{
	const char *p;

#ifndef WIN32
	appendPQExpBufferChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			fprintf(stderr,
					_("shell command argument contains a newline or carriage return: \"%s\"\n"),
					str);
			exit(EXIT_FAILURE);
		}

		if (*p == '\'')
			appendPQExpBufferStr(buf, "'\"'\"'");
		else
			appendPQExpBufferChar(buf, *p);
	}
	appendPQExpBufferChar(buf, '\'');
#else							/* WIN32 */
	int			backslash_run_length = 0;

	/*
	 * A Windows system() argument experiences two layers of interpretation.
	 * First, cmd.exe interprets the string.  Its behavior is undocumented,
	 * but a caret escapes any byte except LF or CR that would otherwise have
	 * special meaning.  Handling of a caret before LF or CR differs between
	 * "cmd.exe /c" and other modes, and it is unusable here.
	 *
	 * Second, the new process parses its command line to construct argv (see
	 * https://msdn.microsoft.com/en-us/library/17w5ykft.aspx).  This treats
	 * backslash-double quote sequences specially.
	 */
	appendPQExpBufferStr(buf, "^\"");
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			fprintf(stderr,
					_("shell command argument contains a newline or carriage return: \"%s\"\n"),
					str);
			exit(EXIT_FAILURE);
		}

		/* Change N backslashes before a double quote to 2N+1 backslashes. */
		if (*p == '"')
		{
			while (backslash_run_length)
			{
				appendPQExpBufferStr(buf, "^\\");
				backslash_run_length--;
			}
			appendPQExpBufferStr(buf, "^\\");
		}
		else if (*p == '\\')
			backslash_run_length++;
		else
			backslash_run_length = 0;

		/*
		 * Decline to caret-escape the most mundane characters, to ease
		 * debugging and lest we approach the command length limit.
		 */
		if (!((*p >= 'a' && *p <= 'z') ||
			  (*p >= 'A' && *p <= 'Z') ||
			  (*p >= '0' && *p <= '9')))
			appendPQExpBufferChar(buf, '^');
		appendPQExpBufferChar(buf, *p);
	}

	/*
	 * Change N backslashes at end of argument to 2N backslashes, because they
	 * precede the double quote that terminates the argument.
	 */
	while (backslash_run_length)
	{
		appendPQExpBufferStr(buf, "^\\");
		backslash_run_length--;
	}
	appendPQExpBufferStr(buf, "^\"");
#endif   /* WIN32 */
}


/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value, in a keyword/pair value in a libpq connection
 * string
 */
void
appendConnStrVal(PQExpBuffer buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string is one or more plain ASCII characters, no need to quote
	 * it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = true;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
		needquotes = false;
	}

	if (needquotes)
	{
		appendPQExpBufferChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendPQExpBufferChar(buf, '\\');

			appendPQExpBufferChar(buf, *str);
			str++;
		}
		appendPQExpBufferChar(buf, '\'');
	}
	else
		appendPQExpBufferStr(buf, str);
}


/*
 * Append a psql meta-command that connects to the given database with the
 * then-current connection's user, host and port.
 */
void
appendPsqlMetaConnect(PQExpBuffer buf, const char *dbname)
{
	const char *s;
	bool		complex;

	/*
	 * If the name is plain ASCII characters, emit a trivial "\connect "foo"".
	 * For other names, even many not technically requiring it, skip to the
	 * general case.  No database has a zero-length name.
	 */
	complex = false;
	for (s = dbname; *s; s++)
	{
		if (*s == '\n' || *s == '\r')
		{
			fprintf(stderr,
					_("database name contains a newline or carriage return: \"%s\"\n"),
					dbname);
			exit(EXIT_FAILURE);
		}

		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			complex = true;
		}
	}

	appendPQExpBufferStr(buf, "\\connect ");
	if (complex)
	{
		PQExpBufferData connstr;

		initPQExpBuffer(&connstr);
		appendPQExpBuffer(&connstr, "dbname=");
		appendConnStrVal(&connstr, dbname);

		appendPQExpBuffer(buf, "-reuse-previous=on ");

		/*
		 * As long as the name does not contain a newline, SQL identifier
		 * quoting satisfies the psql meta-command parser.  Prefer not to
		 * involve psql-interpreted single quotes, which behaved differently
		 * before PostgreSQL 9.2.
		 */
		appendPQExpBufferStr(buf, fmtId(connstr.data));

		termPQExpBuffer(&connstr);
	}
	else
		appendPQExpBufferStr(buf, fmtId(dbname));
	appendPQExpBufferChar(buf, '\n');
}


/*
 * Convert a bytea value (presented as raw bytes) to an SQL string literal
 * and append it to the given buffer.  We assume the specified
 * standard_conforming_strings setting.
 *
 * This is needed in situations where we do not have a PGconn available.
 * Where we do, PQescapeByteaConn is a better choice.
 */
void
appendByteaLiteral(PQExpBuffer buf, const unsigned char *str, size_t length,
				   bool std_strings)
{
	const unsigned char *source = str;
	char	   *target;

	static const char hextbl[] = "0123456789abcdef";

	/*
	 * This implementation is hard-wired to produce hex-format output. We do
	 * not know the server version the output will be loaded into, so making
	 * an intelligent format choice is impossible.  It might be better to
	 * always use the old escaped format.
	 */
	if (!enlargePQExpBuffer(buf, 2 * length + 5))
		return;

	target = buf->data + buf->len;
	*target++ = '\'';
	if (!std_strings)
		*target++ = '\\';
	*target++ = '\\';
	*target++ = 'x';

	while (length-- > 0)
	{
		unsigned char c = *source++;

		*target++ = hextbl[(c >> 4) & 0xF];
		*target++ = hextbl[c & 0xF];
	}

	/* Write the terminating quote and NUL character. */
	*target++ = '\'';
	*target = '\0';

	buf->len = target - buf->data;
}


/*
 * Deconstruct the text representation of a 1-dimensional Postgres array
 * into individual items.
 *
 * On success, returns true and sets *itemarray and *nitems to describe
 * an array of individual strings.  On parse failure, returns false;
 * *itemarray may exist or be NULL.
 *
 * NOTE: free'ing itemarray is sufficient to deallocate the working storage.
 */
bool
parsePGArray(const char *atext, char ***itemarray, int *nitems)
{
	int			inputlen;
	char	  **items;
	char	   *strings;
	int			curitem;

	/*
	 * We expect input in the form of "{item,item,item}" where any item is
	 * either raw data, or surrounded by double quotes (in which case embedded
	 * characters including backslashes and quotes are backslashed).
	 *
	 * We build the result as an array of pointers followed by the actual
	 * string data, all in one malloc block for convenience of deallocation.
	 * The worst-case storage need is not more than one pointer and one
	 * character for each input character (consider "{,,,,,,,,,,}").
	 */
	*itemarray = NULL;
	*nitems = 0;
	inputlen = strlen(atext);
	if (inputlen < 2 || atext[0] != '{' || atext[inputlen - 1] != '}')
		return false;			/* bad input */
	items = (char **) malloc(inputlen * (sizeof(char *) + sizeof(char)));
	if (items == NULL)
		return false;			/* out of memory */
	*itemarray = items;
	strings = (char *) (items + inputlen);

	atext++;					/* advance over initial '{' */
	curitem = 0;
	while (*atext != '}')
	{
		if (*atext == '\0')
			return false;		/* premature end of string */
		items[curitem] = strings;
		while (*atext != '}' && *atext != ',')
		{
			if (*atext == '\0')
				return false;	/* premature end of string */
			if (*atext != '"')
				*strings++ = *atext++;	/* copy unquoted data */
			else
			{
				/* process quoted substring */
				atext++;
				while (*atext != '"')
				{
					if (*atext == '\0')
						return false;	/* premature end of string */
					if (*atext == '\\')
					{
						atext++;
						if (*atext == '\0')
							return false;		/* premature end of string */
					}
					*strings++ = *atext++;		/* copy quoted data */
				}
				atext++;
			}
		}
		*strings++ = '\0';
		if (*atext == ',')
			atext++;
		curitem++;
	}
	if (atext[1] != '\0')
		return false;			/* bogus syntax (embedded '}') */
	*nitems = curitem;
	return true;
}


/*
 * Build GRANT/REVOKE command(s) for an object.
 *
 *	name: the object name, in the form to use in the commands (already quoted)
 *	subname: the sub-object name, if any (already quoted); NULL if none
 *	nspname: the namespace the object is in (NULL if none); not pre-quoted
 *	type: the object type (as seen in GRANT command: must be one of
 *		TABLE, SEQUENCE, FUNCTION, LANGUAGE, SCHEMA, DATABASE, TABLESPACE,
 *		FOREIGN DATA WRAPPER, SERVER, or LARGE OBJECT)
 *	acls: the ACL string fetched from the database
 *	owner: username of object owner (will be passed through fmtId); can be
 *		NULL or empty string to indicate "no owner known"
 *	prefix: string to prefix to each generated command; typically empty
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 *
 * Note: when processing a default ACL, prefix is "ALTER DEFAULT PRIVILEGES "
 * or something similar, and name is an empty string.
 *
 * Note: beware of passing a fmtId() result directly as 'name' or 'subname',
 * since this routine uses fmtId() internally.
 */
bool
buildACLCommands(const char *name, const char *subname, const char *nspname,
				 const char *type, const char *acls, const char *owner,
				 const char *prefix, int remoteVersion,
				 PQExpBuffer sql)
{
	bool		ok = true;
	char	  **aclitems;
	int			naclitems;
	int			i;
	PQExpBuffer grantee,
				grantor,
				privs,
				privswgo;
	PQExpBuffer firstsql,
				secondsql;
	bool		found_owner_privs = false;

	if (strlen(acls) == 0)
		return true;			/* object has default permissions */

	/* treat empty-string owner same as NULL */
	if (owner && *owner == '\0')
		owner = NULL;

	if (!parsePGArray(acls, &aclitems, &naclitems))
	{
		if (aclitems)
			free(aclitems);
		return false;
	}

	grantee = createPQExpBuffer();
	grantor = createPQExpBuffer();
	privs = createPQExpBuffer();
	privswgo = createPQExpBuffer();

	/*
	 * At the end, these two will be pasted together to form the result. But
	 * the owner privileges need to go before the other ones to keep the
	 * dependencies valid.  In recent versions this is normally the case, but
	 * in old versions they come after the PUBLIC privileges and that results
	 * in problems if we need to run REVOKE on the owner privileges.
	 */
	firstsql = createPQExpBuffer();
	secondsql = createPQExpBuffer();

	/*
	 * Always start with REVOKE ALL FROM PUBLIC, so that we don't have to
	 * wire-in knowledge about the default public privileges for different
	 * kinds of objects.
	 */
	appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
	if (subname)
		appendPQExpBuffer(firstsql, "(%s)", subname);
	appendPQExpBuffer(firstsql, " ON %s ", type);
	if (nspname && *nspname)
		appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
	appendPQExpBuffer(firstsql, "%s FROM PUBLIC;\n", name);

	/* Scan individual ACL items */
	for (i = 0; i < naclitems; i++)
	{
		if (!parseAclItem(aclitems[i], type, name, subname, remoteVersion,
						  grantee, grantor, privs, privswgo))
		{
			ok = false;
			break;
		}

		if (grantor->len == 0 && owner)
			printfPQExpBuffer(grantor, "%s", owner);

		if (privs->len > 0 || privswgo->len > 0)
		{
			if (owner
				&& strcmp(grantee->data, owner) == 0
				&& strcmp(grantor->data, owner) == 0)
			{
				found_owner_privs = true;

				/*
				 * For the owner, the default privilege level is ALL WITH
				 * GRANT OPTION (only ALL prior to 7.4).
				 */
				if (strcmp(privswgo->data, "ALL") != 0)
				{
					appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
					if (subname)
						appendPQExpBuffer(firstsql, "(%s)", subname);
					appendPQExpBuffer(firstsql, " ON %s ", type);
					if (nspname && *nspname)
						appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
					appendPQExpBuffer(firstsql, "%s FROM %s;\n",
									  name, fmtId(grantee->data));
					if (privs->len > 0)
					{
						appendPQExpBuffer(firstsql,
										  "%sGRANT %s ON %s ",
										  prefix, privs->data, type);
						if (nspname && *nspname)
							appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
						appendPQExpBuffer(firstsql,
										  "%s TO %s;\n",
										  name, fmtId(grantee->data));
					}
					if (privswgo->len > 0)
					{
						appendPQExpBuffer(firstsql,
										  "%sGRANT %s ON %s ",
										  prefix, privswgo->data, type);
						if (nspname && *nspname)
							appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
						appendPQExpBuffer(firstsql,
										  "%s TO %s WITH GRANT OPTION;\n",
										  name, fmtId(grantee->data));
					}
				}
			}
			else
			{
				/*
				 * Otherwise can assume we are starting from no privs.
				 */
				if (grantor->len > 0
					&& (!owner || strcmp(owner, grantor->data) != 0))
					appendPQExpBuffer(secondsql, "SET SESSION AUTHORIZATION %s;\n",
									  fmtId(grantor->data));

				if (privs->len > 0)
				{
					appendPQExpBuffer(secondsql, "%sGRANT %s ON %s ",
									  prefix, privs->data, type);
					if (nspname && *nspname)
						appendPQExpBuffer(secondsql, "%s.", fmtId(nspname));
					appendPQExpBuffer(secondsql, "%s TO ", name);
					if (grantee->len == 0)
						appendPQExpBufferStr(secondsql, "PUBLIC;\n");
					else if (strncmp(grantee->data, "group ",
									 strlen("group ")) == 0)
						appendPQExpBuffer(secondsql, "GROUP %s;\n",
									fmtId(grantee->data + strlen("group ")));
					else
						appendPQExpBuffer(secondsql, "%s;\n", fmtId(grantee->data));
				}
				if (privswgo->len > 0)
				{
					appendPQExpBuffer(secondsql, "%sGRANT %s ON %s ",
									  prefix, privswgo->data, type);
					if (nspname && *nspname)
						appendPQExpBuffer(secondsql, "%s.", fmtId(nspname));
					appendPQExpBuffer(secondsql, "%s TO ", name);
					if (grantee->len == 0)
						appendPQExpBufferStr(secondsql, "PUBLIC");
					else if (strncmp(grantee->data, "group ",
									 strlen("group ")) == 0)
						appendPQExpBuffer(secondsql, "GROUP %s",
									fmtId(grantee->data + strlen("group ")));
					else
						appendPQExpBufferStr(secondsql, fmtId(grantee->data));
					appendPQExpBufferStr(secondsql, " WITH GRANT OPTION;\n");
				}

				if (grantor->len > 0
					&& (!owner || strcmp(owner, grantor->data) != 0))
					appendPQExpBufferStr(secondsql, "RESET SESSION AUTHORIZATION;\n");
			}
		}
	}

	/*
	 * If we didn't find any owner privs, the owner must have revoked 'em all
	 */
	if (!found_owner_privs && owner)
	{
		appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
		if (subname)
			appendPQExpBuffer(firstsql, "(%s)", subname);
		appendPQExpBuffer(firstsql, " ON %s ", type);
		if (nspname && *nspname)
			appendPQExpBuffer(firstsql, "%s.", fmtId(nspname));
		appendPQExpBuffer(firstsql, "%s FROM %s;\n",
						  name, fmtId(owner));
	}

	destroyPQExpBuffer(grantee);
	destroyPQExpBuffer(grantor);
	destroyPQExpBuffer(privs);
	destroyPQExpBuffer(privswgo);

	appendPQExpBuffer(sql, "%s%s", firstsql->data, secondsql->data);
	destroyPQExpBuffer(firstsql);
	destroyPQExpBuffer(secondsql);

	free(aclitems);

	return ok;
}

/*
 * Build ALTER DEFAULT PRIVILEGES command(s) for single pg_default_acl entry.
 *
 *	type: the object type (TABLES, FUNCTIONS, etc)
 *	nspname: schema name, or NULL for global default privileges
 *	acls: the ACL string fetched from the database
 *	owner: username of privileges owner (will be passed through fmtId)
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 */
bool
buildDefaultACLCommands(const char *type, const char *nspname,
						const char *acls, const char *owner,
						int remoteVersion,
						PQExpBuffer sql)
{
	bool		result;
	PQExpBuffer prefix;

	prefix = createPQExpBuffer();

	/*
	 * We incorporate the target role directly into the command, rather than
	 * playing around with SET ROLE or anything like that.  This is so that a
	 * permissions error leads to nothing happening, rather than changing
	 * default privileges for the wrong user.
	 */
	appendPQExpBuffer(prefix, "ALTER DEFAULT PRIVILEGES FOR ROLE %s ",
					  fmtId(owner));
	if (nspname)
		appendPQExpBuffer(prefix, "IN SCHEMA %s ", fmtId(nspname));

	result = buildACLCommands("", NULL, NULL,
							  type, acls, owner,
							  prefix->data, remoteVersion,
							  sql);

	destroyPQExpBuffer(prefix);

	return result;
}

/*
 * This will parse an aclitem string, having the general form
 *		username=privilegecodes/grantor
 * or
 *		group groupname=privilegecodes/grantor
 * (the /grantor part will not be present if pre-7.4 database).
 *
 * The returned grantee string will be the dequoted username or groupname
 * (preceded with "group " in the latter case).  The returned grantor is
 * the dequoted grantor name or empty.  Privilege characters are decoded
 * and split between privileges with grant option (privswgo) and without
 * (privs).
 *
 * Note: for cross-version compatibility, it's important to use ALL when
 * appropriate.
 */
static bool
parseAclItem(const char *item, const char *type,
			 const char *name, const char *subname, int remoteVersion,
			 PQExpBuffer grantee, PQExpBuffer grantor,
			 PQExpBuffer privs, PQExpBuffer privswgo)
{
	char	   *buf;
	bool		all_with_go = true;
	bool		all_without_go = true;
	char	   *eqpos;
	char	   *slpos;
	char	   *pos;

	buf = strdup(item);
	if (!buf)
		return false;

	/* user or group name is string up to = */
	eqpos = copyAclUserName(grantee, buf);
	if (*eqpos != '=')
	{
		free(buf);
		return false;
	}

	/* grantor may be listed after / */
	slpos = strchr(eqpos + 1, '/');
	if (slpos)
	{
		*slpos++ = '\0';
		slpos = copyAclUserName(grantor, slpos);
		if (*slpos != '\0')
		{
			if (buf)
				free(buf);
			return false;
		}
	}
	else
		resetPQExpBuffer(grantor);

	/* privilege codes */
#define CONVERT_PRIV(code, keywd) \
do { \
	if ((pos = strchr(eqpos + 1, code))) \
	{ \
		if (*(pos + 1) == '*') \
		{ \
			AddAcl(privswgo, keywd, subname); \
			all_without_go = false; \
		} \
		else \
		{ \
			AddAcl(privs, keywd, subname); \
			all_with_go = false; \
		} \
	} \
	else \
		all_with_go = all_without_go = false; \
} while (0)

	resetPQExpBuffer(privs);
	resetPQExpBuffer(privswgo);

	if (strcmp(type, "TABLE") == 0 || strcmp(type, "SEQUENCE") == 0 ||
		strcmp(type, "TABLES") == 0 || strcmp(type, "SEQUENCES") == 0)
	{
		CONVERT_PRIV('r', "SELECT");

		if (strcmp(type, "SEQUENCE") == 0 ||
			strcmp(type, "SEQUENCES") == 0)
			/* sequence only */
			CONVERT_PRIV('U', "USAGE");
		else
		{
			/* table only */
			CONVERT_PRIV('a', "INSERT");
			CONVERT_PRIV('x', "REFERENCES");
			/* rest are not applicable to columns */
			if (subname == NULL)
			{
				CONVERT_PRIV('d', "DELETE");
				CONVERT_PRIV('t', "TRIGGER");
				if (remoteVersion >= 80400)
					CONVERT_PRIV('D', "TRUNCATE");
			}
		}

		/* UPDATE */
		CONVERT_PRIV('w', "UPDATE");
	}
	else if (strcmp(type, "FUNCTION") == 0 ||
			 strcmp(type, "FUNCTIONS") == 0)
		CONVERT_PRIV('X', "EXECUTE");
	else if (strcmp(type, "LANGUAGE") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "SCHEMA") == 0)
	{
		CONVERT_PRIV('C', "CREATE");
		CONVERT_PRIV('U', "USAGE");
	}
	else if (strcmp(type, "DATABASE") == 0)
	{
		CONVERT_PRIV('C', "CREATE");
		CONVERT_PRIV('c', "CONNECT");
		CONVERT_PRIV('T', "TEMPORARY");
	}
	else if (strcmp(type, "TABLESPACE") == 0)
		CONVERT_PRIV('C', "CREATE");
	else if (strcmp(type, "TYPE") == 0 ||
			 strcmp(type, "TYPES") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN DATA WRAPPER") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN SERVER") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN TABLE") == 0)
		CONVERT_PRIV('r', "SELECT");
	else if (strcmp(type, "LARGE OBJECT") == 0)
	{
		CONVERT_PRIV('r', "SELECT");
		CONVERT_PRIV('w', "UPDATE");
	}
	else if (strcmp(type, "PROTOCOL") == 0)
	{
		CONVERT_PRIV('r', "SELECT");
		CONVERT_PRIV('a', "INSERT");
	}
	else
		abort();

#undef CONVERT_PRIV

	if (all_with_go)
	{
		resetPQExpBuffer(privs);
		printfPQExpBuffer(privswgo, "ALL");
		if (subname)
			appendPQExpBuffer(privswgo, "(%s)", subname);
	}
	else if (all_without_go)
	{
		resetPQExpBuffer(privswgo);
		printfPQExpBuffer(privs, "ALL");
		if (subname)
			appendPQExpBuffer(privs, "(%s)", subname);
	}

	free(buf);

	return true;
}

/*
 * Transfer a user or group name starting at *input into the output buffer,
 * dequoting if needed.  Returns a pointer to just past the input name.
 * The name is taken to end at an unquoted '=' or end of string.
 */
static char *
copyAclUserName(PQExpBuffer output, char *input)
{
	resetPQExpBuffer(output);

	while (*input && *input != '=')
	{
		/*
		 * If user name isn't quoted, then just add it to the output buffer
		 */
		if (*input != '"')
			appendPQExpBufferChar(output, *input++);
		else
		{
			/* Otherwise, it's a quoted username */
			input++;
			/* Loop until we come across an unescaped quote */
			while (!(*input == '"' && *(input + 1) != '"'))
			{
				if (*input == '\0')
					return input;		/* really a syntax error... */

				/*
				 * Quoting convention is to escape " as "".  Keep this code in
				 * sync with putid() in backend's acl.c.
				 */
				if (*input == '"' && *(input + 1) == '"')
					input++;
				appendPQExpBufferChar(output, *input++);
			}
			input++;
		}
	}
	return input;
}

/*
 * Append a privilege keyword to a keyword list, inserting comma if needed.
 */
static void
AddAcl(PQExpBuffer aclbuf, const char *keyword, const char *subname)
{
	if (aclbuf->len > 0)
		appendPQExpBufferChar(aclbuf, ',');
	appendPQExpBufferStr(aclbuf, keyword);
	if (subname)
		appendPQExpBuffer(aclbuf, "(%s)", subname);
}


/*
 * processSQLNamePattern
 *
 * Scan a wildcard-pattern string and generate appropriate WHERE clauses
 * to limit the set of objects returned.  The WHERE clauses are appended
 * to the already-partially-constructed query in buf.  Returns whether
 * any clause was added.
 *
 * conn: connection query will be sent to (consulted for escaping rules).
 * buf: output parameter.
 * pattern: user-specified pattern option, or NULL if none ("*" is implied).
 * have_where: true if caller already emitted "WHERE" (clauses will be ANDed
 * onto the existing WHERE clause).
 * force_escape: always quote regexp special characters, even outside
 * double quotes (else they are quoted only between double quotes).
 * schemavar: name of query variable to match against a schema-name pattern.
 * Can be NULL if no schema.
 * namevar: name of query variable to match against an object-name pattern.
 * altnamevar: NULL, or name of an alternative variable to match against name.
 * visibilityrule: clause to use if we want to restrict to visible objects
 * (for example, "pg_catalog.pg_table_is_visible(p.oid)").  Can be NULL.
 *
 * Formatting note: the text already present in buf should end with a newline.
 * The appended text, if any, will end with one too.
 */
bool
processSQLNamePattern(PGconn *conn, PQExpBuffer buf, const char *pattern,
					  bool have_where, bool force_escape,
					  const char *schemavar, const char *namevar,
					  const char *altnamevar, const char *visibilityrule)
{
	PQExpBufferData schemabuf;
	PQExpBufferData namebuf;
	int			encoding = PQclientEncoding(conn);
	bool		inquotes;
	const char *cp;
	int			i;
	bool		added_clause = false;

#define WHEREAND() \
	(appendPQExpBufferStr(buf, have_where ? "  AND " : "WHERE "), \
	 have_where = true, added_clause = true)

	if (pattern == NULL)
	{
		/* Default: select all visible objects */
		if (visibilityrule)
		{
			WHEREAND();
			appendPQExpBuffer(buf, "%s\n", visibilityrule);
		}
		return added_clause;
	}

	initPQExpBuffer(&schemabuf);
	initPQExpBuffer(&namebuf);

	/*
	 * Parse the pattern, converting quotes and lower-casing unquoted letters.
	 * Also, adjust shell-style wildcard characters into regexp notation.
	 *
	 * We surround the pattern with "^(...)$" to force it to match the whole
	 * string, as per SQL practice.  We have to have parens in case the string
	 * contains "|", else the "^" and "$" will be bound into the first and
	 * last alternatives which is not what we want.
	 *
	 * Note: the result of this pass is the actual regexp pattern(s) we want
	 * to execute.  Quoting/escaping into SQL literal format will be done
	 * below using appendStringLiteralConn().
	 */
	appendPQExpBufferStr(&namebuf, "^(");

	inquotes = false;
	cp = pattern;

	while (*cp)
	{
		char		ch = *cp;

		if (ch == '"')
		{
			if (inquotes && cp[1] == '"')
			{
				/* emit one quote, stay in inquotes mode */
				appendPQExpBufferChar(&namebuf, '"');
				cp++;
			}
			else
				inquotes = !inquotes;
			cp++;
		}
		else if (!inquotes && isupper((unsigned char) ch))
		{
			appendPQExpBufferChar(&namebuf,
								  pg_tolower((unsigned char) ch));
			cp++;
		}
		else if (!inquotes && ch == '*')
		{
			appendPQExpBufferStr(&namebuf, ".*");
			cp++;
		}
		else if (!inquotes && ch == '?')
		{
			appendPQExpBufferChar(&namebuf, '.');
			cp++;
		}
		else if (!inquotes && ch == '.')
		{
			/* Found schema/name separator, move current pattern to schema */
			resetPQExpBuffer(&schemabuf);
			appendPQExpBufferStr(&schemabuf, namebuf.data);
			resetPQExpBuffer(&namebuf);
			appendPQExpBufferStr(&namebuf, "^(");
			cp++;
		}
		else if (ch == '$')
		{
			/*
			 * Dollar is always quoted, whether inside quotes or not. The
			 * reason is that it's allowed in SQL identifiers, so there's a
			 * significant use-case for treating it literally, while because
			 * we anchor the pattern automatically there is no use-case for
			 * having it possess its regexp meaning.
			 */
			appendPQExpBufferStr(&namebuf, "\\$");
			cp++;
		}
		else
		{
			/*
			 * Ordinary data character, transfer to pattern
			 *
			 * Inside double quotes, or at all times if force_escape is true,
			 * quote regexp special characters with a backslash to avoid
			 * regexp errors.  Outside quotes, however, let them pass through
			 * as-is; this lets knowledgeable users build regexp expressions
			 * that are more powerful than shell-style patterns.
			 */
			if ((inquotes || force_escape) &&
				strchr("|*+?()[]{}.^$\\", ch))
				appendPQExpBufferChar(&namebuf, '\\');
			i = PQmblen(cp, encoding);
			while (i-- && *cp)
			{
				appendPQExpBufferChar(&namebuf, *cp);
				cp++;
			}
		}
	}

	/*
	 * Now decide what we need to emit.  We may run under a hostile
	 * search_path, so qualify EVERY name.  Note there will be a leading "^("
	 * in the patterns in any case.
	 */
	if (namebuf.len > 2)
	{
		/* We have a name pattern, so constrain the namevar(s) */

		appendPQExpBufferStr(&namebuf, ")$");
		/* Optimize away a "*" pattern */
		if (strcmp(namebuf.data, "^(.*)$") != 0)
		{
			WHEREAND();
			if (altnamevar)
			{
				appendPQExpBuffer(buf,
								  "(%s OPERATOR(pg_catalog.~) ", namevar);
				appendStringLiteralConn(buf, namebuf.data, conn);
				appendPQExpBuffer(buf,
								  "\n        OR %s OPERATOR(pg_catalog.~) ",
								  altnamevar);
				appendStringLiteralConn(buf, namebuf.data, conn);
				appendPQExpBufferStr(buf, ")\n");
			}
			else
			{
				appendPQExpBuffer(buf, "%s OPERATOR(pg_catalog.~) ", namevar);
				appendStringLiteralConn(buf, namebuf.data, conn);
				appendPQExpBufferChar(buf, '\n');
			}
		}
	}

	if (schemabuf.len > 2)
	{
		/* We have a schema pattern, so constrain the schemavar */

		appendPQExpBufferStr(&schemabuf, ")$");
		/* Optimize away a "*" pattern */
		if (strcmp(schemabuf.data, "^(.*)$") != 0 && schemavar)
		{
			WHEREAND();
			appendPQExpBuffer(buf, "%s OPERATOR(pg_catalog.~) ", schemavar);
			appendStringLiteralConn(buf, schemabuf.data, conn);
			appendPQExpBufferChar(buf, '\n');
		}
	}
	else
	{
		/* No schema pattern given, so select only visible objects */
		if (visibilityrule)
		{
			WHEREAND();
			appendPQExpBuffer(buf, "%s\n", visibilityrule);
		}
	}

	termPQExpBuffer(&schemabuf);
	termPQExpBuffer(&namebuf);

	return added_clause;
#undef WHEREAND
}

/*
 * Escape any backslashes in given string (from initdb.c)
 */
char *
escape_backslashes(const char *src, bool quotes_too)
{
	int			len = strlen(src),
				i,
				j;
	char	   *result = malloc(len * 2 + 1);

	for (i = 0, j = 0; i < len; i++)
	{
		if ((src[i]) == '\\' || ((src[i]) == '\'' && quotes_too))
			result[j++] = '\\';
		result[j++] = src[i];
	}
	result[j] = '\0';
	return result;
}

/*
 * Escape backslashes and apostrophes in EXTERNAL TABLE format strings.
 *
 * The fmtopts field of a pg_exttable tuple has an odd encoding -- it is
 * partially parsed and contains "string" values that aren't legal SQL.
 * Each string value is delimited by apostrophes and is usually, but not
 * always, a single character.	The fmtopts field is typically something
 * like {delimiter '\x09' null '\N' escape '\'} or
 * {delimiter ',' null '' escape '\' quote '''}.  Each backslash and
 * apostrophe in a string must be escaped and each string must be
 * prepended with an 'E' denoting an "escape syntax" string.
 *
 * Usage note: A field value containing an apostrophe followed by a space
 * will throw this algorithm off -- it presumes no embedded spaces.
 */
char *
escape_fmtopts_string(const char *src)
{
	int			len = strlen(src);
	int			i;
	int			j;
	char	   *result = malloc(len * 2 + 1);
	bool		inString = false;

	for (i = 0, j = 0; i < len; i++)
	{
		switch (src[i])
		{
			case '\'':
				if (inString)
				{
					/*
					 * Escape apostrophes *within* the string.	If the
					 * apostrophe is at the end of the source string or is
					 * followed by a space, it is presumed to be a closing
					 * apostrophe and is not escaped.
					 */
					if ((i + 1) == len || src[i + 1] == ' ')
						inString = false;
					else
						result[j++] = '\\';
				}
				else
				{
					result[j++] = 'E';
					inString = true;
				}
				break;
			case '\\':
				result[j++] = '\\';
				break;
		}

		result[j++] = src[i];
	}

	result[j] = '\0';
	return result;
}

/*
 * Tokenize a fmtopts string (for use with 'custom' formatters)
 * i.e. convert it to: a = b, format.
 * (e.g.:  formatter E'fixedwidth_in null E' ' preserve_blanks E'on')
 */
char *
custom_fmtopts_string(const char *src)
{
	PQExpBufferData result;
	char *srcdup;
	char *to_free;
	char *find_res;
	char *srcdup_end;
	int last;

	if (!src)
		return NULL;

	to_free = srcdup = pg_strdup(src);
	srcdup_end = srcdup + strlen(srcdup);
	initPQExpBuffer(&result);

	while (srcdup)
	{
		/* find first word (a) */
		find_res = strchr(srcdup, ' ');
		if (!find_res)
			break;
		*find_res = '\0';
		appendPQExpBufferStr(&result, srcdup);
		/* skip space */
		srcdup = find_res + 1;
		/* remove E if E' */
		if ((strlen(srcdup) > 2) && (srcdup[0] == 'E') && (srcdup[1] == '\''))
			srcdup++;
		/* add " = " */
		appendPQExpBuffer(&result, " = ");
		/* find second word (b) until second '
		   find \' combinations and ignore them */
		find_res = strchr(srcdup + 1, '\'');
		while (find_res && (*(find_res - 1) == '\\') /* ignore \' */)
		{
			find_res = strchr(find_res + 1, '\'');
		}
		if (!find_res)
			break;
		find_res++;
		*find_res = '\0';
		appendPQExpBufferStr(&result, srcdup);
		srcdup = find_res;
		/* move to the next token if exists and add ',' */
		if (find_res < srcdup_end - 1)
		{
			srcdup = find_res + 1;
			appendPQExpBuffer(&result, ",");
		}
	}

	/* fix string - remove trailing ',' or '=' */
	last = strlen(result.data) - 1;
	if (last >= 0 && (result.data[last] == ',' || result.data[last] == '='))
		result.data[last] = '\0';

	pg_free(to_free);
	return result.data;
}

/*
 * buildShSecLabelQuery
 *
 * Build a query to retrieve security labels for a shared object.
 * The object is identified by its OID plus the name of the catalog
 * it can be found in (e.g., "pg_database" for database names).
 * The query is appended to "sql".  (We don't execute it here so as to
 * keep this file free of assumptions about how to deal with SQL errors.)
 */
void
buildShSecLabelQuery(PGconn *conn, const char *catalog_name, Oid objectId,
					 PQExpBuffer sql)
{
	appendPQExpBuffer(sql,
					  "SELECT provider, label FROM pg_catalog.pg_shseclabel "
					  "WHERE classoid = 'pg_catalog.%s'::pg_catalog.regclass "
					  "AND objoid = '%u'", catalog_name, objectId);
}

/*
 * emitShSecLabels
 *
 * Construct SECURITY LABEL commands using the data retrieved by the query
 * generated by buildShSecLabelQuery, and append them to "buffer".
 * Here, the target object is identified by its type name (e.g. "DATABASE")
 * and its name (not pre-quoted).
 */
void
emitShSecLabels(PGconn *conn, PGresult *res, PQExpBuffer buffer,
				const char *objtype, const char *objname)
{
	int			i;

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *provider = PQgetvalue(res, i, 0);
		char	   *label = PQgetvalue(res, i, 1);

		/* must use fmtId result before calling it again */
		appendPQExpBuffer(buffer,
						  "SECURITY LABEL FOR %s ON %s",
						  fmtId(provider), objtype);
		appendPQExpBuffer(buffer,
						  " %s IS ",
						  fmtId(objname));
		appendStringLiteralConn(buffer, label, conn);
		appendPQExpBufferStr(buffer, ";\n");
	}
}


void
simple_string_list_append(SimpleStringList *list, const char *val)
{
	SimpleStringListCell *cell;

	/* this calculation correctly accounts for the null trailing byte */
	cell = (SimpleStringListCell *)
		pg_malloc(sizeof(SimpleStringListCell) + strlen(val));

	cell->next = NULL;
	strcpy(cell->val, val);

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

bool
simple_string_list_member(SimpleStringList *list, const char *val)
{
	SimpleStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (strcmp(cell->val, val) == 0)
			return true;
	}
	return false;
}

/*
 * Detect whether the given GUC variable is of GUC_LIST_QUOTE type.
 *
 * It'd be better if we could inquire this directly from the backend; but even
 * if there were a function for that, it could only tell us about variables
 * currently known to guc.c, so that it'd be unsafe for extensions to declare
 * GUC_LIST_QUOTE variables anyway.  Lacking a solution for that, it doesn't
 * seem worth the work to do more than have this list, which must be kept in
 * sync with the variables actually marked GUC_LIST_QUOTE in guc.c.
 */
bool
variable_is_guc_list_quote(const char *name)
{
	if (pg_strcasecmp(name, "temp_tablespaces") == 0 ||
		pg_strcasecmp(name, "temp_spill_files_tablespaces") == 0 ||
		pg_strcasecmp(name, "session_preload_libraries") == 0 ||
		pg_strcasecmp(name, "shared_preload_libraries") == 0 ||
		pg_strcasecmp(name, "local_preload_libraries") == 0 ||
		pg_strcasecmp(name, "search_path") == 0)
		return true;
	else
		return false;
}

/*
 * SplitGUCList --- parse a string containing identifiers or file names
 *
 * This is used to split the value of a GUC_LIST_QUOTE GUC variable, without
 * presuming whether the elements will be taken as identifiers or file names.
 * See comparable code in src/backend/utils/adt/varlena.c.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').  Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: receives a malloc'd, null-terminated array of pointers to
 *			  identifiers within rawstring.  Caller should free this
 *			  even on error return.
 *
 * Returns true if okay, false if there is a syntax error in the string.
 */
bool
SplitGUCList(char *rawstring, char separator,
			 char ***namelist)
{
	char	   *nextp = rawstring;
	bool		done = false;
	char	  **nextptr;

	/*
	 * Since we disallow empty identifiers, this is a conservative
	 * overestimate of the number of pointers we could need.  Allow one for
	 * list terminator.
	 */
	*namelist = nextptr = (char **)
		pg_malloc((strlen(rawstring) / 2 + 2) * sizeof(char *));
	*nextptr = NULL;

	while (isspace((unsigned char) *nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == '"')
		{
			/* Quoted name --- collapse quote-quote pairs */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '"');
				if (endp == NULL)
					return false;	/* mismatched quotes */
				if (endp[1] != '"')
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !isspace((unsigned char) *nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */
		}

		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to output array
		 */
		*nextptr++ = curname;

		/* Loop back if we didn't reach end of string */
	} while (!done);

	*nextptr = NULL;
	return true;
}
