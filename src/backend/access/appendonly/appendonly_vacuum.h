#ifndef APPENDONLY_VACUUM_H
#define APPENDONLY_VACUUM_H

#include <commands/vacuum.h>
#include <utils/relcache.h>
#include <storage/buf.h>

void ao_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy);

#endif   /* APPENDONLY_VACUUM_H */
