#include "postgres.h"

#include "optimizer/orca_guc.h"

#include <float.h>
#include <limits.h>

#include "utils/guc.h"
#include "utils/guc_tables.h"

#define ORCA_GUC_PROCESS_NO_SYNC_FLAG(flags) \
	((flags & GUC_GPDB_NEED_SYNC) ? flags : (flags | GUC_GPDB_NO_SYNC))

struct config_bool configure_names_bool_orca[] = {
	/* End-of-list marker */
	{{NULL, 0, 0, NULL, NULL}, NULL, false, NULL, NULL}};

struct config_int configure_names_int_orca[] = {
	/* End-of-list marker */
	{{NULL, 0, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL}};

struct config_real configure_names_real_orca[] = {
	/* End-of-list marker */
	{{NULL, 0, 0, NULL, NULL}, NULL, 0.0, 0.0, 0.0, NULL, NULL}};

struct config_string configure_names_string_orca[] = {
	/* End-of-list marker */
	{{NULL, 0, 0, NULL, NULL}, NULL, NULL, NULL, NULL}};

struct config_enum configure_names_enum_orca[] = {
	/* End-of-list marker */
	{{NULL, 0, 0, NULL, NULL}, NULL, 0, NULL, NULL, NULL}};

void
orca_guc_define()
{
	int i;

	for (i = 0; configure_names_bool_orca[i].gen.name; i++)
	{
		struct config_bool *conf = &configure_names_bool_orca[i];

		DefineCustomBoolVariable(
			conf->gen.name, conf->gen.short_desc, conf->gen.long_desc,
			conf->variable, conf->boot_val, conf->gen.context,
			ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags), conf->check_hook,
			conf->assign_hook, conf->show_hook);
	}

	for (i = 0; configure_names_int_orca[i].gen.name; i++)
	{
		struct config_int *conf = &configure_names_int_orca[i];

		DefineCustomIntVariable(
			conf->gen.name, conf->gen.short_desc, conf->gen.long_desc,
			conf->variable, conf->boot_val, conf->min, conf->max,
			conf->gen.context, ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
			conf->check_hook, conf->assign_hook, conf->show_hook);
	}

	for (i = 0; configure_names_real_orca[i].gen.name; i++)
	{
		struct config_real *conf = &configure_names_real_orca[i];

		DefineCustomRealVariable(
			conf->gen.name, conf->gen.short_desc, conf->gen.long_desc,
			conf->variable, conf->boot_val, conf->min, conf->max,
			conf->gen.context, ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags),
			conf->check_hook, conf->assign_hook, conf->show_hook);
	}

	for (i = 0; configure_names_string_orca[i].gen.name; i++)
	{
		struct config_string *conf = &configure_names_string_orca[i];

		DefineCustomStringVariable(
			conf->gen.name, conf->gen.short_desc, conf->gen.long_desc,
			conf->variable, conf->boot_val, conf->gen.context,
			ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags), conf->check_hook,
			conf->assign_hook, conf->show_hook);
	}

	for (i = 0; configure_names_enum_orca[i].gen.name; i++)
	{
		struct config_enum *conf = &configure_names_enum_orca[i];

		DefineCustomEnumVariable(
			conf->gen.name, conf->gen.short_desc, conf->gen.long_desc,
			conf->variable, conf->boot_val, conf->options, conf->gen.context,
			ORCA_GUC_PROCESS_NO_SYNC_FLAG(conf->gen.flags), conf->check_hook,
			conf->assign_hook, conf->show_hook);
	}
}
