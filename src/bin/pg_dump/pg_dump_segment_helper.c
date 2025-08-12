/*-------------------------------------------------------------------------
 *
 * pg_dump_segment_helper.c
 *	  pg_dump_segment_helper provides functions which saves data from stdin
 *	  to files and restores data from files to stdout
 *
 *-------------------------------------------------------------------------
 */

#include "parallel.h"

#include <ctype.h>

#include "pg_backup.h"
#include "pg_backup_archiver.h"
#include "pg_backup_utils.h"
#include "pg_dump_segment_helper.h"

static int runRestore_Archive(const char *inputFileSpec);
static int runRestore_PlainText(const char *inputFileSpec);
#ifdef HAVE_LIBZ
static int runRestore_CompressedPlainText(const char *inputFileSpec);
#endif

static void setupDumpWorkerDummy(Archive *AH, RestoreOptions *ropt);

static void addDumpableObject(Archive *AH);
static void ConfigureForClearData(Archive *AH);
static int	getDumpFromStdin(Archive *AH, void *dcontext);

int
onSegmentHelper_RunDump(const char *inputFileSpec, int compressLevel, ArchiveFormat format)
{
	Archive		   *AH;
	RestoreOptions *ropt;

	/* Open the output file */
	AH = CreateArchive(
		inputFileSpec, format, compressLevel, archModeWrite, setupDumpWorkerDummy
	);

	/* Register the cleanup hook */
	on_exit_close_archive(AH);

	if (AH == NULL)
		exit_horribly(NULL, "could not open output file \"%s\" for writing\n", inputFileSpec);

	ConfigureForClearData(AH);

	addDumpableObject(AH);

	/*
	 * Set up options info to ensure we dump what we want.
	 */
	ropt = NewRestoreOptions();
	ropt->filename = inputFileSpec;
	ropt->compression = compressLevel == -1 ? 0 : compressLevel;

	((ArchiveHandle *)AH)->ropt = ropt;

	if (format == archNull)
		RestoreArchive(AH);

	CloseArchive(AH);

	return 0;
}

int
onSegmentHelper_RunRestore(const char *inputFileSpec, int compressLevel, ArchiveFormat format)
{
	if (format != archNull)
		return runRestore_Archive(inputFileSpec);
#ifdef HAVE_LIBZ
	if (compressLevel != 0)
		return runRestore_CompressedPlainText(inputFileSpec);
#endif
	return runRestore_PlainText(inputFileSpec);
}

static int
runRestore_Archive(const char *inputFileSpec)
{
	RestoreOptions *opts;
	Archive		   *AH;
	int				exitCode = 0;

	AH = OpenArchive(inputFileSpec, archUnknown);

	on_exit_close_archive(AH);

	ConfigureForClearData(AH);

	opts = NewRestoreOptions();

	SetArchiveRestoreOptions(AH, opts);
	RestoreArchive(AH);

	if (AH->n_errors)
		fprintf(stderr, _("WARNING: errors ignored on restore: %d\n"), AH->n_errors);

	exitCode = AH->n_errors ? 1 : 0;

	CloseArchive(AH);

	return exitCode;
}

static int
runRestore_PlainText(const char *inputFileSpec)
{
	int	  exitCode = 0;
	FILE *readFd = fopen(inputFileSpec, "r");
	if (!readFd)
	{
		fprintf(stderr, _("Couldn't open plain text file for reading\n"));
		return 1;
	}

	const int	bufferSize = 1024;
	const char *buffer[bufferSize];

	size_t readLen;
	while ((readLen = fread(buffer, sizeof(char), bufferSize, readFd)) > 0)
	{
		if (fwrite(buffer, readLen, sizeof(char), stdout) == 0)
		{
			fprintf(stderr, _("Error while writing to stdout\n"));
			exitCode = 1;
			break;
		}
	}

	fclose(readFd);

	return exitCode;
}

#ifdef HAVE_LIBZ
static int
runRestore_CompressedPlainText(const char *inputFileSpec)
{
	int	   exitCode = 0;
	gzFile readFd = gzopen(inputFileSpec, "rb");
	if (!readFd)
	{
		fprintf(stderr, _("Couldn't open .gz file for reading\n"));
		return 1;
	}

	const int	bufferSize = 8192;
	const char *buffer[bufferSize];

	size_t readLen = 0;
	while ((readLen = GZREAD(buffer, sizeof(char), bufferSize, readFd)) > 0)
	{
		if (fwrite(buffer, readLen, sizeof(char), stdout) == 0)
		{
			fprintf(stderr, _("Error while writing to stdout\n"));
			exitCode = 1;
			break;
		}
	}

	GZCLOSE(readFd);

	return exitCode;
}
#endif

static void
addDumpableObject(Archive *AH)
{
	DumpableObject dobj = {.name = "", .dumpId = 1};
	ArchiveHandle *AHH = (ArchiveHandle *)AH;

	ArchiveEntry(
		AH, dobj.catId, dobj.dumpId, dobj.name, NULL, NULL, "", false, "", SECTION_DATA, "", "",
		NULL, NULL, 0, getDumpFromStdin, NULL
	);

	AHH->toc->next->reqs = REQ_DATA;
}

/* We dont want any comments or SET operators in segments' dumps */
static void
ConfigureForClearData(Archive *AH)
{
	ArchiveHandle *AHH = (ArchiveHandle *)AH;

	AHH->noTocComments = 1;
	AHH->dataOnly = 1;
}

static int
getDumpFromStdin(Archive *AH, void *dcontext)
{
	char copybuf[1000];
	while (fgets(copybuf, sizeof copybuf, stdin) != NULL)
		WriteData(AH, copybuf, strlen(copybuf));

	return 1;
}

static void
setupDumpWorkerDummy(Archive *AH, RestoreOptions *ropt)
{
}
