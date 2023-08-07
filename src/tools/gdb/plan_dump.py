import gdb

PLANGEN_PLANNER = gdb.parse_and_eval("PLANGEN_PLANNER")

class NodeTag:
	gdb_type = gdb.lookup_type('NodeTag')

class Sequence:
	gdb_type = gdb.lookup_type('Sequence').pointer()

class Node:
	gdb_type = gdb.lookup_type('Node').pointer()

class Plan:
	gdb_type = gdb.lookup_type('Plan').pointer()

class RangeTblEntry:
	gdb_type = gdb.lookup_type("RangeTblEntry").pointer()

class Slice:
	gdb_type = gdb.lookup_type('Slice').pointer()

class Gang:
	GANGTYPE_UNALLOCATED = gdb.parse_and_eval("GANGTYPE_UNALLOCATED")       # /* a root slice executed by the qDisp */
	GANGTYPE_ENTRYDB_READER = gdb.parse_and_eval("GANGTYPE_ENTRYDB_READER")    # /* a 1-gang with read access to the entry db */
	GANGTYPE_SINGLETON_READER = gdb.parse_and_eval("GANGTYPE_SINGLETON_READER")	# /* a 1-gang to read the segment dbs */
	GANGTYPE_PRIMARY_READER = gdb.parse_and_eval("GANGTYPE_PRIMARY_READER")    # /* a 1-gang or N-gang to read the segment dbs */
	GANGTYPE_PRIMARY_WRITER = gdb.parse_and_eval("GANGTYPE_PRIMARY_WRITER")	# /* the N-gang that can update the segment dbs */

class Flow:
	FLOW_UNDEFINED = gdb.parse_and_eval("FLOW_UNDEFINED")			#	/* used prior to calculation of type of derived flow */
	FLOW_SINGLETON = gdb.parse_and_eval("FLOW_SINGLETON")			#	/* flow has single stream */
	FLOW_REPLICATED = gdb.parse_and_eval("FLOW_REPLICATED")			#	/* flow is replicated across IOPs */
	FLOW_PARTITIONED = gdb.parse_and_eval("FLOW_PARTITIONED")		#	/* flow is partitioned across IOPs */

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
	
# what with bool, addrs and ints

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
			# /*
			# * - for motion nodes we want to display the sender segments
			# *   count, it can be fetched from lefttree;
			# * - for non-motion nodes the segments count can be fetched
			# *   from either lefttree or plan itself, they should be the
			# *   same;
			# * - there are also nodes like Hash that might have NULL
			# *   plan->flow but non-NULL lefttree->flow, so we can use
			# *   whichever that's available.
			# */
			fplan = plan

			while True:
				# if fplan["type"] == Motion.gdb_type:
				tag = str(fplan["type"])
				if tag == "T_Motion":
					fplan = fplan["lefttree"]
					continue

				if fplan["flow"] != 0:
					break

				# /* No flow on this node. Dig into child. */
				if fplan["lefttree"] != 0:
					fplan = fplan["lefttree"]
					continue

				# if (IsA(fplan, Append))
				# {
				# 	Append	   *aplan = (Append *) fplan;

				# 	if (aplan->appendplans)
				# 	{
				# 		fplan = (Plan *) linitial(aplan->appendplans);
				# 		continue;
				# 	}
				# }
				# elog(WARNING, "could not find flow for node of type %d", fplan->type);
				break

			if fplan["flow"] == 0:
				# /* no flow and no subplan; shouldn't happen */
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
	sliceIndex = estate["es_sliceTable"]["localSlice"] #???
	sliceTable = estate["es_sliceTable"]
	if sliceTable != 0 and sliceIndex >= 0 and sliceIndex < List.list_length(sliceTable["slices"]):
		return List.list_nth(sliceTable["slices"], sliceIndex)


class Motion(object):
	MOTIONTYPE_HASH = gdb.parse_and_eval("MOTIONTYPE_HASH") #	0	/* Use hashing to select a segindex destination */
	MOTIONTYPE_FIXED = gdb.parse_and_eval("MOTIONTYPE_FIXED") #	1	/* Send tuples to a fixed set of segindexes */
	MOTIONTYPE_EXPLICIT = gdb.parse_and_eval("MOTIONTYPE_EXPLICIT") # 2		/* Send tuples to the segment explicitly specified in their segid column */
	gdb_type = gdb.lookup_type('Motion').pointer()
	def __init__(self, val, state, pstmt, currentSlice):
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
		motion_recv = -1
		if self.__parentSlice == 0:
			motion_recv = 1
		else:
			motion_recv = int(self.__parentSlice["gangSize"])

		sname = ""
		if typ == Motion.MOTIONTYPE_HASH:
			sname = "Redistribute Motion"
		elif typ == Motion.MOTIONTYPE_FIXED:
			if bool(self.__mot["isBroadcast"]) == True:
				sname = "Broadcast Motion"
			elif self.__plan["lefttree"]["flow"]["locustype"] == LocusType.CdbLocusType_Replicated:
				sname = "Explicit Gather Motion"
				# scaleFactor = 1
				motion_recv = 1
			else:
				sname = "Gather Motion"
				# scaleFactor = 1
				motion_recv = 1
		elif typ == Motion.MOTIONTYPE_EXPLICIT:
			sname = "Explicit Redistribute Motion"
			# motion_recv = getgpsegmentCount()


		if self.__pstmt["planGen"] == PLANGEN_PLANNER:
			slice = self.__currentSlice
			if bool(slice["directDispatch"]["isDirectDispatch"]) == True:
				#/* Special handling on direct dispatch */
				motion_snd = List.list_length(slice["directDispatch"]["contentIds"])
			elif self.__plan["lefttree"]["flow"]["flotype"] == Flow.FLOW_SINGLETON:
				# /* For SINGLETON we always display sender size as 1 */
				motion_snd = 1
			else:
				# /* Otherwise find out sender size from outer plan */
				motion_snd = int(self.__plan["lefttree"]["flow"]["numsegments"])

			#######
			if self.__mot["motionType"] == Motion.MOTIONTYPE_FIXED and bool(self.__mot["isBroadcast"]) == False:
				# /* In Gather Motion always display receiver size as 1 */
				motion_recv = 1
			else:
				# /* Otherwise find out receiver size from plan */
				motion_recv = int(self.__plan["flow"]["numsegments"])
		
		return '%s %d:%d %s' % (sname, motion_snd, motion_recv, show_dispatch_info(self.__currentSlice, self.__plan, self.__pstmt))

###
class ShareInputScan(object):
	gdb_type = gdb.lookup_type("ShareInputScan")

	def __init__(self, node, currentSlice):
		self.__sics = node.cast(ShareInputScan.gdb_type)
		self.__currentSlice = currentSlice

	def print_node(self):
		slice_id = -1
		if self.__currentSlice != 0:
			# does it calculates correctly, motions rewrites current slice???
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

	def print_node(self):
		id = int(self.scan["scanrelid"])
		relanme = self.__rtableMap[id]
		return "%s on %s" % (self.__typ, relanme)

class DynamicSeqScan(Scan):
	gdb_type = gdb.lookup_type("DynamicSeqScan").pointer()

	def __init__(self, node, rtableMap):
		super(DynamicSeqScan, self).__init__(node, rtableMap, "DynamicSeqScan")

class SeqScan(Scan):
	gdb_type = gdb.lookup_type("SeqScan").pointer()

	def __init__(self, node, rtableMap):
		super(SeqScan, self).__init__(node, rtableMap, "SeqScan")

class IndexScan(Scan):
	gdb_type = gdb.lookup_type("IndexScan").pointer()

	def __init__(self, node, rtableMap):
		super(IndexScan, self).__init__(node, rtableMap, "IndexScan")

class PlanDumperCmd(gdb.Command):
	"""Print the plan nodes like pg explain"""

	def __init__(self):
		super(PlanDumperCmd, self).__init__(
			"plan_dump_cmd", gdb.COMMAND_USER
		)
		self.result = ""
		self.tabCnt = 0

	def walk_node(self, nodePtr):
		nodeTag = str(nodePtr["type"])
		if nodeTag.startswith("T_"):
			nodeTag = nodeTag[2:]
		else:
			raise Exception('unknown nodeTag: % %', nodeTag, nodePtr)
		
		# seems the calculation of current slice is invalid
		nodeStr = nodeTag
		# we cant do something like node["type"] == Motion.gdb_type seems
		if nodeTag == "Motion":
			nodeStr = Motion(nodePtr, self.__state, self.__pstmt, self.__currentSlice).print_node()
		elif nodeTag == "ShareInputScan":
			nodeStr = ShareInputScan(nodePtr, self.__currentSlice).print_node()
		elif nodeTag == "DynamicSeqScan":
			nodeStr = DynamicSeqScan(nodePtr, self.rtableMap).print_node()
		elif nodeTag == "SeqScan":
			nodeStr = SeqScan(nodePtr, self.rtableMap).print_node()
		elif nodeTag == "IndexScan":
			nodeStr = IndexScan(nodePtr, self.rtableMap).print_node()
		elif nodeTag == "PartitionSelector":
			nodeStr = PartitionSelector(nodePtr, self.reloidMap).print_node()

		self.result += "\t" * self.tabCnt + "-> " + nodeStr + "\n"

		self.tabCnt = self.tabCnt + 1

		planPtr =  nodePtr.cast(Plan.gdb_type)
		
		if planPtr["lefttree"]:
			self.walk_node(planPtr["lefttree"])
		if planPtr["righttree"]:
			self.walk_node(planPtr["righttree"])

		# it should be extended
		if nodeTag == "Sequence":
			seq = nodePtr.cast(Sequence.gdb_type)
			subplans = seq["subplans"].cast(List.gdb_type)
			head = subplans["head"]
			while head != 0:
				self.walk_node(head["data"]["ptr_value"].cast(Node.gdb_type))
				head = head["next"]

		self.tabCnt = self.tabCnt - 1

		return self.result

	def complete(self, text, word):
		# We expect the argument passed to be a symbol so fallback to the
		# internal tab-completion handler for symbols
		return gdb.COMPLETE_SYMBOL

	def invoke(self, args, from_tty):
		# We can pass args here and use Python CLI utilities like argparse
		# to do argument parsing. Based on ExplainPrintPlan

		queryDesc = gdb.parse_and_eval(args)
		self.__state = queryDesc["estate"]
		self.__pstmt = queryDesc["plannedstmt"]
		#from LocallyExecutingSliceIndex: return (!estate->es_sliceTable ? 0 : estate->es_sliceTable->localSlice);
		self.__currentSlice = getCurrentSlice(self.__state, int(self.__state["es_sliceTable"]["localSlice"])).cast(Slice.gdb_type)
		# *sliceTable = planstate->state->es_sliceTable;
		# seems no need, it must be the same as queryDesc.estate
		# for motion, esatete  == plansatate->state it's a global 
		self.__state = queryDesc["planstate"]["state"]

		self.rtableMap = {}
		self.reloidMap = {}


		# *(RangeTblEntry*)queryDesc.plannedstmt.rtable.head.data.ptr_value 
		rtableIter = self.__pstmt["rtable"]["head"]

		i = 1
		while rtableIter != 0:
			rte = rtableIter["data"]["ptr_value"].cast(RangeTblEntry.gdb_type)
			name = str(rte["eref"]["aliasname"])
			self.reloidMap[int(rte["relid"])] = name
			# start with 1 instead of original 0 and other stuff with scanrelid - 1
			# From explain_target_rel refname = (char *) list_nth(es->rtable_names, rti - 1);
			self.rtableMap[i] = name
			i += 1
			rtableIter = rtableIter["next"]
			# print(int(rte["relid"]), name)

		res = self.walk_node(queryDesc["plannedstmt"]["planTree"])
		with open("/data/Output.txt", "w") as text_file:
			text_file.write(res)

PlanDumperCmd()
