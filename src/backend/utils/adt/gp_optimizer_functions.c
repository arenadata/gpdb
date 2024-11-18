/*
 * gp_optimizer_functions.c
 *    Defines builtin transformation functions for the optimizer.
 *
 * enable_xform: This function wraps EnableXform.
 *
 * disable_xform: This function wraps DisableXform.
 *
 * gp_opt_version: This function wraps LibraryVersion. 
 *
 * Copyright(c) 2012 - present, EMC/Greenplum
 */

#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"

typedef Datum (*gp_opt_version_func) (void);

/*
 * Loads optimizer function from a shared library. If library is not presented
 * or doesn't contain the requested function or any of its dependencies,
 * returns NULL.
 */
static PGFunction
gp_optimizer_load_function(char *funcname)
{
	volatile PGFunction func = NULL;
	int32		savedInterruptHoldoffCount = InterruptHoldoffCount;
	MemoryContext oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		func = load_external_function("$libdir/gp_orca", funcname, false, NULL);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		InterruptHoldoffCount = savedInterruptHoldoffCount;
		FlushErrorState();
		func = NULL;
	}
	PG_END_TRY();

	return func;
}

/*
 * A stub function to call if no optimizer function is found.
 */
static Datum
gp_optimizer_function_stub()
{
	return CStringGetTextDatum("Server has been compiled without ORCA");
}

/*
* Enables transformations in the optimizer.
*/
Datum
enable_xform(PG_FUNCTION_ARGS)
{
	PGFunction	func = gp_optimizer_load_function("EnableXform");

	if (func)
		return func(fcinfo);
	else
		return gp_optimizer_function_stub();
}

/*
* Disables transformations in the optimizer.
*/
Datum
disable_xform(PG_FUNCTION_ARGS)
{
	PGFunction	func = gp_optimizer_load_function("DisableXform");

	if (func)
		return func(fcinfo);
	else
		return gp_optimizer_function_stub();
}

/*
* Returns the optimizer and gpos library versions.
*/
Datum
gp_opt_version(PG_FUNCTION_ARGS pg_attribute_unused())
{
	gp_opt_version_func func =
		(gp_opt_version_func) gp_optimizer_load_function("LibraryVersion");

	if (func)
		return func();
	else
		return gp_optimizer_function_stub();
}
