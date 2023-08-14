import gdb

PLANGEN_PLANNER = gdb.parse_and_eval("PLANGEN_PLANNER")

class RangeTblEntry:
	gdb_type = gdb.lookup_type("RangeTblEntry").pointer()

class Slice:
	gdb_type = gdb.lookup_type('Slice').pointer()

# class NodeTag:
# 	gdb_type = gdb.lookup_type('NodeTag')

class Node:
	gdb_type = gdb.lookup_type('Node').pointer()

class TargetEntry:
	gdb_type = gdb.lookup_type('TargetEntry').pointer()

class Plan:
	gdb_type = gdb.lookup_type('Plan').pointer()

class PlanState:
	gdb_type = gdb.lookup_type('PlanState').pointer()

class SubPlan:
	gdb_type = gdb.lookup_type('SubPlan').pointer()

class SubPlanState:
	gdb_type = gdb.lookup_type('SubPlanState').pointer()

class ModifyTable:
	gdb_type = gdb.lookup_type('ModifyTable').pointer()

class ModifyTableState:
	gdb_type = gdb.lookup_type('ModifyTableState').pointer()

class Append:
	gdb_type = gdb.lookup_type('Append').pointer()

class AppendState:
	gdb_type = gdb.lookup_type('AppendState').pointer()

class MergeAppend:
	gdb_type = gdb.lookup_type('MergeAppend').pointer()

class MergeAppendState:
	gdb_type = gdb.lookup_type('MergeAppendState').pointer()

class Sequence:
	gdb_type = gdb.lookup_type('Sequence').pointer()

class SequenceState:
	gdb_type = gdb.lookup_type('SequenceState').pointer()

class BitmapAnd:
	gdb_type = gdb.lookup_type('BitmapAnd').pointer()

class BitmapAndState:
	gdb_type = gdb.lookup_type('BitmapAndState').pointer()

class BitmapOr:
	gdb_type = gdb.lookup_type('BitmapOr').pointer()

class BitmapOrState:
	gdb_type = gdb.lookup_type('BitmapOrState').pointer()

class SubqueryScan:
	gdb_type = gdb.lookup_type('SubqueryScan').pointer()

class SubqueryScanState:
	gdb_type = gdb.lookup_type('SubqueryScanState').pointer()

class Result:
	gdb_type = gdb.lookup_type('Result').pointer()


class Gang:
	GANGTYPE_UNALLOCATED = gdb.parse_and_eval("GANGTYPE_UNALLOCATED")			# a root slice executed by the qDisp */
	GANGTYPE_ENTRYDB_READER = gdb.parse_and_eval("GANGTYPE_ENTRYDB_READER")		# a 1-gang with read access to the entry db */
	GANGTYPE_SINGLETON_READER = gdb.parse_and_eval("GANGTYPE_SINGLETON_READER")	# a 1-gang to read the segment dbs */
	GANGTYPE_PRIMARY_READER = gdb.parse_and_eval("GANGTYPE_PRIMARY_READER")		# a 1-gang or N-gang to read the segment dbs */
	GANGTYPE_PRIMARY_WRITER = gdb.parse_and_eval("GANGTYPE_PRIMARY_WRITER")		# the N-gang that can update the segment dbs */

class SetOp:
	gdb_type = gdb.lookup_type("SetOp").pointer()
	# Strategies enum
	SETOP_SORTED = gdb.parse_and_eval('SETOP_SORTED')
	SETOP_HASHED = gdb.parse_and_eval('SETOP_HASHED')
	# Commands enum
	SETOPCMD_INTERSECT = gdb.parse_and_eval('SETOPCMD_INTERSECT')
	SETOPCMD_INTERSECT_ALL = gdb.parse_and_eval('SETOPCMD_INTERSECT_ALL')
	SETOPCMD_EXCEPT = gdb.parse_and_eval('SETOPCMD_EXCEPT')
	SETOPCMD_EXCEPT_ALL = gdb.parse_and_eval('SETOPCMD_EXCEPT_ALL')

	@staticmethod
	def print_node(node):
		setOp = node.cast(SetOp.gdb_type)
		strategy = setOp["strategy"]
		strategyStr = "SetOp ???"
		if strategy == SetOp.SETOP_SORTED:
			strategyStr = "SetOp"
		elif strategy == SetOp.SETOP_HASHED:
			strategyStr = "HashSetOp"

		command = setOp["cmd"]
		commandStr = "???"
		if command == SetOp.SETOPCMD_INTERSECT:
			commandStr = "Intersect"
		elif command == SetOp.SETOPCMD_INTERSECT_ALL:
			commandStr = "Intersect All"
		elif command == SetOp.SETOPCMD_EXCEPT:
			commandStr = "Except"
		elif command == SetOp.SETOPCMD_EXCEPT_ALL:
			commandStr = "Except All"

		return "%s %s" % (strategyStr, commandStr)

class Agg:
	gdb_type = gdb.lookup_type('Agg').pointer()
	# Strategies
	AGG_PLAIN = gdb.parse_and_eval('AGG_PLAIN')
	AGG_SORTED = gdb.parse_and_eval('AGG_SORTED')
	AGG_HASHED = gdb.parse_and_eval('AGG_HASHED')

class Flow:
	FLOW_UNDEFINED = gdb.parse_and_eval("FLOW_UNDEFINED")		# used prior to calculation of type of derived flow */
	FLOW_SINGLETON = gdb.parse_and_eval("FLOW_SINGLETON")		# flow has single stream */
	FLOW_REPLICATED = gdb.parse_and_eval("FLOW_REPLICATED")		# flow is replicated across IOPs */
	FLOW_PARTITIONED = gdb.parse_and_eval("FLOW_PARTITIONED")	# flow is partitioned across IOPs */

class LocusType:
	CdbLocusType_Null = gdb.parse_and_eval("CdbLocusType_Null")
	CdbLocusType_Entry = gdb.parse_and_eval("CdbLocusType_Entry")
	CdbLocusType_SingleQE = gdb.parse_and_eval("CdbLocusType_SingleQE")
	CdbLocusType_General = gdb.parse_and_eval("CdbLocusType_General")
	CdbLocusType_SegmentGeneral = gdb.parse_and_eval("CdbLocusType_SegmentGeneral")
	CdbLocusType_Replicated = gdb.parse_and_eval("CdbLocusType_Replicated")
	CdbLocusType_Hashed = gdb.parse_and_eval("CdbLocusType_Hashed")
	CdbLocusType_HashedOJ = gdb.parse_and_eval("CdbLocusType_HashedOJ")
	CdbLocusType_Strewn = gdb.parse_and_eval("CdbLocusType_Strewn")
	CdbLocusType_End = gdb.parse_and_eval("CdbLocusType_End")

class Join:
	gdb_type = gdb.lookup_type('Join').pointer()

class NestLoop:
	gdb_type = gdb.lookup_type('NestLoop').pointer()

class IndexScan:
	gdb_type = gdb.lookup_type('IndexScan').pointer()

class IndexOnlyScan:
	gdb_type = gdb.lookup_type('IndexOnlyScan').pointer()

class BitmapIndexScan:
	gdb_type = gdb.lookup_type('BitmapIndexScan').pointer()

class DynamicBitmapIndexScan:
	gdb_type = gdb.lookup_type('DynamicBitmapIndexScan').pointer()

class List:
	gdb_type = gdb.lookup_type('List').pointer()

	@staticmethod
	def list_nth(lst, n):
		l = lst.cast(List.gdb_type)
		match = l["head"]
		while n > 0:
			match = match["next"]
			n -= 1
		return match["data"]["ptr_value"]
	
	@staticmethod
	def list_length(lst):
		return int(lst.cast(List.gdb_type)["length"])
	
def show_dispatch_info(slice, plan, pstmt):
	if slice == 0:
		return ""

	typ = slice["gangType"]
	segments = 0
	if typ == Gang.GANGTYPE_UNALLOCATED or typ == Gang.GANGTYPE_ENTRYDB_READER:
		segments = 0
	elif typ == Gang.GANGTYPE_PRIMARY_WRITER or typ == Gang.GANGTYPE_PRIMARY_READER or typ == Gang.GANGTYPE_SINGLETON_READER:
		if bool(slice["directDispatch"]["isDirectDispatch"]) == True:
			segments = List.list_length(slice["directDispatch"]["contentIds"])
		elif pstmt["planGen"] == PLANGEN_PLANNER:
			# - for motion nodes we want to display the sender segments
			#   count, it can be fetched from lefttree;
			# - for non-motion nodes the segments count can be fetched
			#   from either lefttree or plan itself, they should be the
			#   same;
			# - there are also nodes like Hash that might have NULL
			#   plan->flow but non-NULL lefttree->flow, so we can use
			#   whichever that's available.
			fplan = plan

			while True:
				tag = str(fplan["type"])
				if tag == "T_Motion":
					fplan = fplan["lefttree"]
					continue

				if fplan["flow"] != 0:
					break

				# No flow on this node. Dig into child.
				if fplan["lefttree"] != 0:
					fplan = fplan["lefttree"]
					continue

				if tag == "T_Append":
					aplan = fplan.cast(Append.gdb_type)

					if aplan["appendplans"] != 0:
						fplan = List.list_nth(aplan["appendplans"], 0).cast(Plan.gdb_type)
						continue
				
				raise "could not find flow for node of type %s" % tag

			if fplan["flow"] == 0:
				# no flow and no subplan; shouldn't happen
				segments = 1
			elif fplan["flow"]["flotype"] == Flow.FLOW_SINGLETON:
				segments = 1
			else:
				segments = int(fplan["flow"]["numsegments"])
		else:
			segments = int(slice["gangSize"])

	if segments == 0:
		return ("slice%d") % int(slice["sliceIndex"])
	else:
		return ("slice%d; segments %d") % (int(slice["sliceIndex"]), segments)

def getCurrentSlice(estate, sliceIndex):
	sliceTable = estate["es_sliceTable"]
	if sliceTable == 0:
		return None
	sliceIndex = estate["es_sliceTable"]["localSlice"]
	if sliceIndex >= 0 and sliceIndex < List.list_length(sliceTable["slices"]):
		return List.list_nth(sliceTable["slices"], sliceIndex)
	return None

class Motion(object):
	MOTIONTYPE_HASH = gdb.parse_and_eval("MOTIONTYPE_HASH") #	0	 Use hashing to select a segindex destination */
	MOTIONTYPE_FIXED = gdb.parse_and_eval("MOTIONTYPE_FIXED") #	1	 Send tuples to a fixed set of segindexes */
	MOTIONTYPE_EXPLICIT = gdb.parse_and_eval("MOTIONTYPE_EXPLICIT") # 2		 Send tuples to the segment explicitly specified in their segid column */
	gdb_type = gdb.lookup_type('Motion').pointer()
	def __init__(self, val, state, pstmt, currentSlice):
		# TODO: current_slice should be fixed
		self.__plan = val
		self.__state = state
		self.__pstmt = pstmt
		self.__mot = val.cast(Motion.gdb_type)
		self.__currentSlice = currentSlice

		slices = self.__state["es_sliceTable"]["slices"]
		self.__currentSlice = List.list_nth(slices, int(self.__mot["motionID"])).cast(Slice.gdb_type)

		parentIdx = int(self.__currentSlice["parentIndex"])
		if parentIdx == -1:
			self.__parentSlice = 0
		else:
			self.__parentSlice = List.list_nth(slices, parentIdx).cast(Slice.gdb_type)

	def print_node(self):
		typ = self.__mot["motionType"]
		motion_snd = int(self.__currentSlice["gangSize"])
		motion_recv = 1
		if self.__parentSlice != 0:
			motion_recv = int(self.__parentSlice["gangSize"])

		sname = "???"
		if typ == Motion.MOTIONTYPE_HASH:
			sname = "Redistribute Motion"
		elif typ == Motion.MOTIONTYPE_FIXED:
			if bool(self.__mot["isBroadcast"]) == True:
				sname = "Broadcast Motion"
			elif self.__plan["lefttree"]["flow"]["locustype"] == LocusType.CdbLocusType_Replicated:
				sname = "Explicit Gather Motion"
				motion_recv = 1
			else:
				sname = "Gather Motion"
				motion_recv = 1
		elif typ == Motion.MOTIONTYPE_EXPLICIT:
			sname = "Explicit Redistribute Motion"
			# motion_recv = getgpsegmentCount() it's not easy to implement analog for core-dump

		if self.__pstmt["planGen"] == PLANGEN_PLANNER:
			slice = self.__currentSlice
			if bool(slice["directDispatch"]["isDirectDispatch"]) == True:
				# Special handling on direct dispatch
				motion_snd = List.list_length(slice["directDispatch"]["contentIds"])
			elif self.__plan["lefttree"]["flow"]["flotype"] == Flow.FLOW_SINGLETON:
				# For SINGLETON we always display sender size as 1
				motion_snd = 1
			else:
				# Otherwise find out sender size from outer plan
				motion_snd = int(self.__plan["lefttree"]["flow"]["numsegments"])

			if self.__mot["motionType"] == Motion.MOTIONTYPE_FIXED and bool(self.__mot["isBroadcast"]) == False:
				# In Gather Motion always display receiver size as 1
				motion_recv = 1
			else:
				# Otherwise find out receiver size from plan
				motion_recv = int(self.__plan["flow"]["numsegments"])
		
		return '%s %d:%d %s' % (sname, motion_snd, motion_recv, show_dispatch_info(self.__currentSlice, self.__plan, self.__pstmt))

class ShareInputScan(object):
	gdb_type = gdb.lookup_type("ShareInputScan")

	def __init__(self, node, currentSlice):
		self.__sics = node.cast(ShareInputScan.gdb_type)
		self.__currentSlice = currentSlice

	def print_node(self):
		slice_id = -1
		if self.__currentSlice != 0:
			# TODO: does it calculates correctly, motions rewrites current slice???
			slice_id = int(self.__currentSlice["sliceIndex"])

		return "ShareInputScan (share slice:id %d:%d)" % (slice_id, int(self.__sics["share_id"]))
	
class PartitionSelector(object):
	gdb_type = gdb.lookup_type("PartitionSelector").pointer()

	def __init__(self, node, reloidMap):
		self._ps = node.cast(PartitionSelector.gdb_type)
		self.reloidMap = reloidMap

	def print_node(self):
		relname = self.reloidMap[int(self._ps["relid"])]
		return "PartitionSelector for %s (dynamic scan id: %d)" % (relname, int(self._ps["scanId"]))

class Scan(object):
	gdb_type = gdb.lookup_type("Scan").pointer()
	__typ = ""

	def __init__(self, node, rtableMap, typ):
		self.scan = node.cast(Scan.gdb_type)
		self.__rtableMap = rtableMap
		self.__typ = typ

	def print_node(self, indexid):
		indexInfo = ""
		if indexid is not None:
			indexInfo = " (used index, indexoid %s)" % str(indexid)
		id = int(self.scan["scanrelid"])
		relanme = self.__rtableMap[id]
		return "%s on %s%s" % (self.__typ, relanme, indexInfo)

class PlanDumperCmd(gdb.Command):
	"""Print the plan nodes like pg explain"""

	def __init__(self):
		super(PlanDumperCmd, self).__init__("plan_dump_cmd", gdb.COMMAND_USER)
		self.result = ""
		self.tabCnt = 0

	def walk_initplans(self, plans, sliceTable):
		"""
		This function (and walk_subplans) is an analog of ExplainSubplans
		from explain.c, but instead of working with the PlanStates (like
		original function does) of plan (the EXPLAIN has all states of
		plan nodes starting from root), this function can't rely on PlanStates,
		because coredump's from segments may contain only part planstate 
		(queryDesc->planstate) nodes of the original plan 
		(queryDesc->plannedstmt->planTree), so this function works with the
		Plan nodes, and it's processing differs (in case of subPlans (not init plans
		the PlanState nodes have the subPlan field, while the Plan nodes does not))
		"""
		head = plans["head"]
		saved_slice = self.__currentSlice
		while head != 0:
			sp = head["data"]["ptr_value"].cast(SubPlan.gdb_type)
			if sliceTable != 0 and sp["qDispSliceId"] > 0:
				self.__currentSlice = List.list_nth(sliceTable["slices"], sp["qDispSliceId"]).cast(Slice.gdb_type)

			self.__curPlanName = sp["plan_name"]
			sp_plan = List.list_nth(self.__pstmt["subplans"], int(sp["plan_id"]) - 1).cast(Plan.gdb_type)
			self.walk_node(sp_plan)
			head = head["next"]
		self.__currentSlice = saved_slice
	
	def walk_subplans(self, plannode):
		tag = str(plannode["type"])
		if plannode["qual"] != 0:
			self.walker(plannode["qual"])
		if plannode["targetlist"] != 0:
			self.walker(plannode["targetlist"])

		# Processing of other node types should be added later.
		if tag == "T_Result":
			resNode = plannode.cast(Result.gdb_type)
			if resNode["resconstantqual"] != 0:
				self.walker(resNode["resconstantqual"])

	def walker(self, node):
		tag = str(node["type"])
		if tag == "T_List":
			listNode = node.cast(List.gdb_type)
			head = listNode["head"]
			while head != 0:
				self.walker(head["data"]["ptr_value"].cast(Node.gdb_type))
				head = head["next"]
		if tag == "T_SubPlan":
			sp = node.cast(SubPlan.gdb_type)
			self.__curPlanName = sp["plan_name"]
			sp_plan = List.list_nth(self.__pstmt["subplans"], int(sp["plan_id"]) - 1).cast(Plan.gdb_type)
			self.walk_node(sp_plan)
		if tag == "T_TargetEntry":
			self.walker(node.cast(TargetEntry.gdb_type)["expr"])

	def walk_member_nodes(self, plans):
		head = plans["head"]
		while head != 0:
			self.walk_node(head["data"]["ptr_value"].cast(Plan.gdb_type))
			head = head["next"]

	def walk_node(self, nodePtr):
		"""This function is an analog of ExplainNode"""
		nodeTag = str(nodePtr["type"])
		planPtr = nodePtr.cast(Plan.gdb_type)
		save_currentSlice = self.__currentSlice
		if nodeTag.startswith("T_"):
			nodeTag = nodeTag[2:]
		else:
			raise Exception('unknown nodeTag: % %', nodeTag, planPtr)
		
		# seems the calculation of current slice is invalid
		nodeStr = nodeTag
		skipOuter = False
		simpleScans = ["SeqScan", "DynamicSeqScan", "ExternalScan", "DynamicIndexScan", "BitmapHeapScan", "DynamicBitmapHeapScan", "TidScan", "SubqueryScan", "FunctionScan", "TableFunctionScan", "ValuesScan", "CteScan", "WorkTableScan", "ForeignScan"]
		if nodeTag == "DML" or nodeTag == "ModifyTable":
			# unified approach for the DML and ModifyTable nodes instead of original code
			operation = str(self.__pstmt["commandType"])[4:] # drop 'CMD_' prefix
			nodeStr += " " + operation
			if nodeTag == "ModifyTable":
				rti = int(planPtr.cast(ModifyTable.gdb_type)["resultRelations"]["head"]["data"]["int_value"])
				nodeStr += (" on %s" % str(self.rtableMap[rti]))
		elif nodeTag == "NestLoop":
			skipOuter = bool(planPtr.cast(NestLoop.gdb_type)["shared_outer"])
		elif nodeTag == "Agg":
			agg = planPtr.cast(Agg.gdb_type)
			strategy = agg["aggstrategy"]
			nodeStr = "Aggregate ???"
			if strategy == Agg.AGG_PLAIN: 
				nodeStr = "Aggregate"
			elif strategy == Agg.AGG_SORTED: 
				nodeStr = "GroupAggregate"
			elif strategy == Agg.AGG_HASHED: 
				nodeStr = "HashAggregate"
		elif nodeTag == "SetOp":
			nodeStr = SetOp.print_node(planPtr)
		elif nodeTag == "Motion":
			nodeStr = Motion(planPtr, self.__state, self.__pstmt, self.__currentSlice).print_node()
		elif nodeTag == "ShareInputScan":
			nodeStr = ShareInputScan(planPtr, self.__currentSlice).print_node()
		elif nodeTag in simpleScans:
			nodeStr = Scan(planPtr, self.rtableMap, nodeTag).print_node(None)
		elif nodeTag == "IndexScan":
			nodeStr = Scan(planPtr, self.rtableMap, nodeTag).print_node(planPtr.cast(IndexScan.gdb_type)["indexid"])
		elif nodeTag == "IndexOnlyScan":
			nodeStr = Scan(planPtr, self.rtableMap, nodeTag).print_node(planPtr.cast(IndexOnlyScan.gdb_type)["indexid"])
		elif nodeTag == "BitmapIndexScan":
			nodeStr = Scan(planPtr, self.rtableMap, nodeTag).print_node(planPtr.cast(BitmapIndexScan.gdb_type)["indexid"])
		elif nodeTag == "DynamicBitmapIndexScan":
			nodeStr = Scan(planPtr, self.rtableMap, nodeTag).print_node(planPtr.cast(DynamicBitmapIndexScan.gdb_type)["indexid"])
		elif nodeTag == "PartitionSelector":
			nodeStr = PartitionSelector(planPtr, self.reloidMap).print_node()
		elif nodeStr in ["NestLoop", "MergeJoin", "HashJoin"]:
			joinType = str(planPtr.cast(Join.gdb_type)["jointype"])[5:]
			nodeStr = "%s %s Join" % (nodeTag, joinType)

		if self.__curPlanName is not None:
			planName = str(self.__curPlanName)
			self.result +=  "\t" * (self.tabCnt-1)+ ("%s (%s)\n" % (planName, show_dispatch_info(self.__currentSlice, planPtr, self.__pstmt)))
			self.__curPlanName = None
		self.result += "\t" * self.tabCnt + "-> " + nodeStr + "\n"
		self.tabCnt = self.tabCnt + 1

		if planPtr["initPlan"] != 0:
			self.walk_initplans(planPtr["initPlan"], self.__state["es_sliceTable"])

		if planPtr["lefttree"] != 0 and skipOuter == False:
			self.walk_node(planPtr["lefttree"])
		elif skipOuter == True:
			self.result += "\t" * self.tabCnt + "-> See first subplan of Hash Join"

		if planPtr["righttree"] != 0:
			self.walk_node(planPtr["righttree"])

		if nodeTag == "ModifyTable":
			self.walk_member_nodes(planPtr.cast(ModifyTable.gdb_type)["plans"])
		elif nodeTag == "Append":
			self.walk_member_nodes(planPtr.cast(Append.gdb_type)["appendplans"])
		elif nodeTag == "MergeAppend":
			self.walk_member_nodes(planPtr.cast(MergeAppend.gdb_type)["mergeplans"])
		elif nodeTag == "Sequence":
			self.walk_member_nodes(planPtr.cast(Sequence.gdb_type)["subplans"])
		elif nodeTag == "BitmapAnd":
			self.walk_member_nodes(planPtr.cast(BitmapAnd.gdb_type)["bitmapplans"])
		elif nodeTag == "BitmapOr":
			self.walk_member_nodes(planPtr.cast(BitmapOr.gdb_type)["bitmapplans"])
		elif nodeTag == "SubqueryScan":
			self.walk_node(planPtr.cast(SubqueryScan.gdb_type)["subplan"])

		self.walk_subplans(planPtr)

		self.tabCnt = self.tabCnt - 1
		self.__currentSlice = save_currentSlice

		return self.result

	def complete(self, text, word):
		# We expect the argument passed to be a symbol so fallback to the
		# internal tab-completion handler for symbols
		return gdb.COMPLETE_SYMBOL

	def invoke(self, args, from_tty):
		# does the commentary needed and also the complete/invoke
		# We can pass args here and use Python CLI utilities like argparse
		# to do argument parsing. Based on ExplainPrintPlan

		queryDesc = gdb.parse_and_eval(args)
		self.__state = queryDesc["estate"]
		self.__pstmt = queryDesc["plannedstmt"]
		self.__curPlanName = ""

		# the logic of the original getCurrentSlice nd LocallyExecutedSliceIndex is embedded into getCurrentSlice
		self.__currentSlice = getCurrentSlice(self.__state, int(self.__state["es_sliceTable"]["localSlice"])).cast(Slice.gdb_type)

		i = 1
		self.rtableMap = {}
		self.reloidMap = {}
		rtableIter = self.__pstmt["rtable"]["head"] # *(RangeTblEntry*)queryDesc.plannedstmt.rtable.head.data.ptr_value
		while rtableIter != 0:
			rte = rtableIter["data"]["ptr_value"].cast(RangeTblEntry.gdb_type)
			name = str(rte["eref"]["aliasname"])
			self.reloidMap[int(rte["relid"])] = name
			# start with 1 instead of original 0 and other stuff with scanrelid - 1
			# From explain_target_rel refname = (char *) list_nth(es->rtable_names, rti - 1);
			self.rtableMap[i] = name
			i += 1
			rtableIter = rtableIter["next"]

		self.walk_node(queryDesc["plannedstmt"]["planTree"])
		# with open("/data/Output.txt", "w") as text_file:
		# 	text_file.write(res)
		print(self.result)
		self.result = ""

PlanDumperCmd()
