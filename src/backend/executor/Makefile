#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for executor
#
# IDENTIFICATION
#    src/backend/executor/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/executor
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)


<<<<<<< HEAD
OBJS = execAmi.o execCurrent.o execExpr.o execExprInterp.o \
       execGrouping.o execIndexing.o execJunk.o \
       execMain.o execParallel.o execPartition.o execProcnode.o \
       execReplication.o execScan.o execSRF.o execTuples.o \
       execUtils.o functions.o instrument.o nodeAppend.o nodeAgg.o \
       nodeBitmapAnd.o nodeBitmapOr.o \
       nodeBitmapHeapscan.o nodeBitmapIndexscan.o \
       nodeCustom.o nodeFunctionscan.o nodeGather.o \
       nodeHash.o nodeHashjoin.o nodeIndexscan.o nodeIndexonlyscan.o \
       nodeLimit.o nodeLockRows.o nodeGatherMerge.o \
       nodeMaterial.o nodeMergeAppend.o nodeMergejoin.o nodeModifyTable.o \
       nodeNestloop.o nodeProjectSet.o nodeRecursiveunion.o nodeResult.o \
       nodeSamplescan.o nodeSeqscan.o nodeSetOp.o nodeSort.o nodeUnique.o \
       nodeValuesscan.o \
       nodeCtescan.o nodeNamedtuplestorescan.o nodeWorktablescan.o \
       nodeSubplan.o nodeSubqueryscan.o nodeTidscan.o \
       nodeForeignscan.o nodeWindowAgg.o tstoreReceiver.o tqueue.o spi.o \
       nodeTableFuncscan.o
=======
OBJS = \
	execAmi.o \
	execCurrent.o \
	execExpr.o \
	execExprInterp.o \
	execGrouping.o \
	execIndexing.o \
	execJunk.o \
	execMain.o \
	execParallel.o \
	execPartition.o \
	execProcnode.o \
	execReplication.o \
	execSRF.o \
	execScan.o \
	execTuples.o \
	execUtils.o \
	functions.o \
	instrument.o \
	nodeAgg.o \
	nodeAppend.o \
	nodeBitmapAnd.o \
	nodeBitmapHeapscan.o \
	nodeBitmapIndexscan.o \
	nodeBitmapOr.o \
	nodeCtescan.o \
	nodeCustom.o \
	nodeForeignscan.o \
	nodeFunctionscan.o \
	nodeGather.o \
	nodeGatherMerge.o \
	nodeGroup.o \
	nodeHash.o \
	nodeHashjoin.o \
	nodeIndexonlyscan.o \
	nodeIndexscan.o \
	nodeLimit.o \
	nodeLockRows.o \
	nodeMaterial.o \
	nodeMergeAppend.o \
	nodeMergejoin.o \
	nodeModifyTable.o \
	nodeNamedtuplestorescan.o \
	nodeNestloop.o \
	nodeProjectSet.o \
	nodeRecursiveunion.o \
	nodeResult.o \
	nodeSamplescan.o \
	nodeSeqscan.o \
	nodeSetOp.o \
	nodeSort.o \
	nodeSubplan.o \
	nodeSubqueryscan.o \
	nodeTableFuncscan.o \
	nodeTidscan.o \
	nodeUnique.o \
	nodeValuesscan.o \
	nodeWindowAgg.o \
	nodeWorktablescan.o \
	spi.o \
	tqueue.o \
	tstoreReceiver.o
>>>>>>> BISECT_HEAD

OBJS += nodeMotion.o \
       nodeShareInputScan.o \
       nodeTableFunction.o \
       nodeSequence.o \
       nodeAssertOp.o \
       nodeSplitUpdate.o \
       nodeTupleSplit.o \
       nodePartitionSelector.o

OBJS += nodeDynamicSeqscan.o \
        nodeDynamicIndexscan.o \
        nodeDynamicBitmapHeapscan.o \
        nodeDynamicBitmapIndexscan.o

include $(top_srcdir)/src/backend/common.mk
