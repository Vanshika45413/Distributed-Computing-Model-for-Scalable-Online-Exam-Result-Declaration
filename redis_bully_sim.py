import time
import random
import threading
import redis
from queue import Queue # For Bully algorithm part
from flask import Flask, jsonify, request
from flask_cors import CORS
import traceback # For detailed exception logging

# --- Configuration ---
NUM_ZONES = 3
SERVERS_PER_CLUSTER = 10
CLUSTERS_PER_ZONE = 6
ZONE_NAMES = {
    0: "North Mumbai",
    1: "Central Mumbai",
    2: "South Mumbai"
}
DATA_CENTERS = {
    "North Mumbai": "Data Center 01",
    "Central Mumbai": "Data Center 02",
    "South Mumbai": "Data Center 03"
}
# Bully Algorithm Config
NUM_REPLICAS_PER_SHARD = 3
ELECTION_TIMEOUT = 0.5
HEARTBEAT_INTERVAL = 2.0
FAILURE_PROBABILITY = 0.1

# --- Locks ---
print_lock = threading.Lock()

def locked_print(*args, **kwargs):
    """Helper function for thread-safe printing."""
    with print_lock:
        print(*args, **kwargs)

# --- Flask App Instance ---
app = Flask(__name__)
CORS(app) # Allow Cross-Origin requests (adjust for production)

# --- Global Variables ---
# Initialize globals to None initially
redis_client = None
primary_load_balancer = None
database_shards = [] # Holds DatabaseShard manager instances

# --- Helper Function for Redis Keys ---
def get_cluster_load_key(cluster_id):
    return f"cluster:load:{cluster_id}"

# --- DatabaseReplica Class (Includes Bully Algorithm) ---
class DatabaseReplica:
    def __init__(self, shard_id, replica_id, all_replica_proxies):
        self.shard_id = shard_id
        self.replica_id = replica_id # My unique ID (0, 1, 2...)
        self.all_replica_proxies = all_replica_proxies # Dict {rep_id: replica_obj} in this shard
        self.state = "UP" # Can be "UP" or "DOWN"
        self.current_leader_id = NUM_REPLICAS_PER_SHARD - 1 # Assume highest ID is leader initially
        self.is_election_active = False
        self.election_lock = threading.Lock() # Protect election state
        self.message_queue = Queue() # Simulate receiving messages
        self.data = {} # Each replica holds a copy of the data (simplified)
        self.ok_received_event = threading.Event()
        # Pre-populate some data specific to the shard
        for i in range(100): # Smaller sample data for replicas
            roll_num = f"R{i:03d}{self.shard_id}"
            self.data[roll_num] = f"Result for {roll_num} from Shard {self.shard_id} (Replica {self.replica_id})"

    def _send_message(self, target_replica_id, message_type, sender_id):
        """Simulates sending a message to another replica's queue."""
        if target_replica_id in self.all_replica_proxies:
            target_replica = self.all_replica_proxies[target_replica_id]
            if target_replica.state == "UP":
                target_replica.message_queue.put((message_type, sender_id))

    def process_messages(self):
        """Process incoming messages from the queue."""
        while not self.message_queue.empty():
            message_type, sender_id = self.message_queue.get()
            if self.state != "UP": continue # Ignore messages if down
            if message_type == "ELECTION": self._handle_election_message(sender_id)
            elif message_type == "OK": self._handle_ok_message(sender_id)
            elif message_type == "COORDINATOR": self._handle_coordinator_message(sender_id)

    def _handle_election_message(self, sender_id):
        """Handles receiving an ELECTION message."""
        if self.replica_id > sender_id:
            self._send_message(sender_id, "OK", self.replica_id)
            self.initiate_election("received election from lower ID")

    def _handle_ok_message(self, sender_id):
        """Handles receiving an OK message."""
        self.ok_received_event.set()

    def _handle_coordinator_message(self, new_leader_id):
        """Handles receiving a COORDINATOR message."""
        with self.election_lock:
            if new_leader_id != self.current_leader_id:
                locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] Acknowledging NEW LEADER: R{new_leader_id}")
                self.current_leader_id = new_leader_id
            self.is_election_active = False

    def initiate_election(self, reason=""):
        """Starts the leader election process."""
        with self.election_lock:
            if self.is_election_active: return False
            if self.state != "UP": return False # Cannot start election if down
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] !!! Initiating ELECTION ({reason}) !!!")
            self.is_election_active = True
            self.ok_received_event.clear()
        higher_replicas_exist = any(rep_id > self.replica_id for rep_id in self.all_replica_proxies)
        if not higher_replicas_exist:
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] No higher IDs, declaring myself leader.")
            self._declare_victory()
            return True
        for rep_id in self.all_replica_proxies:
            if rep_id > self.replica_id:
                self._send_message(rep_id, "ELECTION", self.replica_id)
        timer = threading.Timer(ELECTION_TIMEOUT, self._check_election_result)
        timer.daemon = True # Allow program to exit even if timer is pending
        timer.start()
        return True

    def _check_election_result(self):
        """Called after the timeout to see if OK was received."""
        if not self.is_election_active: return
        if not self.ok_received_event.is_set():
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] Timeout, no OK. Declaring victory!")
            self._declare_victory()
        else:
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] Election timed out, but OK received. Waiting for leader.")
            with self.election_lock: self.is_election_active = False

    def _declare_victory(self):
        """Declares self as leader and sends COORDINATOR messages."""
        with self.election_lock:
            if self.state != "UP": self.is_election_active = False; return
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] +++ Declaring MYSELF (R{self.replica_id}) as LEADER! +++")
            self.current_leader_id = self.replica_id
            self.is_election_active = False
        for rep_id in self.all_replica_proxies:
            if rep_id != self.replica_id:
                self._send_message(rep_id, "COORDINATOR", self.replica_id)

    def get_result(self, roll_number):
        """Simulates reading data from this replica."""
        if self.state != "UP": raise ConnectionError(f"Replica S{self.shard_id}R{self.replica_id} is DOWN")
        time.sleep(random.uniform(0.5, 1.0))
        return self.data.get(roll_number, "Result Not Found")

    def check_leader_health(self):
        """Periodically checks leader status and starts election if needed."""
        if self.state != "UP" or self.is_election_active: return
        leader = self.all_replica_proxies.get(self.current_leader_id)
        if not leader or leader.state == "DOWN":
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] Detected LEADER R{self.current_leader_id} is DOWN!")
            self.initiate_election("leader detected down")

    def simulate_failure(self):
        """Marks this replica as DOWN."""
        if self.state == "UP":
            locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] !!! SIMULATING FAILURE - GOING DOWN !!!")
            self.state = "DOWN"
            with self.election_lock: self.is_election_active = False

    def simulate_recovery(self):
       """Marks replica as UP and potentially starts election."""
       if self.state == "DOWN":
           locked_print(f"[Replica S{self.shard_id}R{self.replica_id}] >>> SIMULATING RECOVERY - COMING UP <<<")
           self.state = "UP"
           self.initiate_election("recovered from failure")


# --- DatabaseShard Class (Manages Replicas) ---
class DatabaseShard:
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.replicas = {}
        self.leader_check_thread = None
        self.message_processor_thread = None
        self.stop_event = threading.Event()
        self.leader_id_lock = threading.Lock()
        self._current_leader_id = NUM_REPLICAS_PER_SHARD - 1
        locked_print(f"[Shard S{self.shard_id}] Initializing {NUM_REPLICAS_PER_SHARD} replicas...")
        for i in range(NUM_REPLICAS_PER_SHARD):
            self.replicas[i] = DatabaseReplica(shard_id, i, self.replicas)
            locked_print(f"  [Shard S{self.shard_id}] Created Replica R{i}")
        self.start_background_tasks()

    def get_current_leader(self):
        """Safely get the current leader replica object."""
        with self.leader_id_lock:
            leader_id = self._current_leader_id
            if leader_id in self.replicas:
                 rep = self.replicas[leader_id]
                 if rep.state == "UP":
                    # Check if the replica itself knows about a different leader
                    if rep.current_leader_id != leader_id:
                         # locked_print(f"[Shard S{self.shard_id}] Updating known leader from R{leader_id} to R{rep.current_leader_id} based on replica knowledge.")
                         self._current_leader_id = rep.current_leader_id
                         leader_id = self._current_leader_id
            leader_replica = self.replicas.get(leader_id)
            if leader_replica and leader_replica.state == "UP":
                return leader_replica
            else:
                # Fallback: find any UP replica (election should correct soon)
                for rep_id_fallback in sorted(self.replicas.keys(), reverse=True): # Prefer higher IDs as temp leader
                    rep_fallback = self.replicas[rep_id_fallback]
                    if rep_fallback.state == "UP":
                        # locked_print(f"[Shard S{self.shard_id}] WARN: Leader R{leader_id} down/unknown, temporarily using backup R{rep_fallback.replica_id}")
                        return rep_fallback
                return None

    def get_result(self, roll_number):
        """Get result from the current leader replica."""
        leader_replica = self.get_current_leader()
        if leader_replica:
            thread_name = threading.current_thread().name
            locked_print(f"      [Shard S{self.shard_id}] (Thread: {thread_name}) Forwarding read for {roll_number} to LEADER Replica R{leader_replica.replica_id}")
            try: return leader_replica.get_result(roll_number)
            except ConnectionError as e:
                locked_print(f"      [Shard S{self.shard_id}] (Thread: {thread_name}) Error contacting leader R{leader_replica.replica_id}: {e}")
                return "Error: Database temporarily unavailable"
        else:
            thread_name = threading.current_thread().name
            locked_print(f"      [Shard S{self.shard_id}] (Thread: {thread_name}) CRITICAL: No UP replicas available for {roll_number}!")
            return "Error: Database unavailable"

    def _run_leader_checks(self):
        """Background task to periodically check leader health and trigger failures."""
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            if self.stop_event.is_set(): break
            up_replicas = [r for r in self.replicas.values() if r.state == "UP"]
            if not up_replicas: continue
            checker = random.choice(up_replicas)
            checker.check_leader_health()
            # Use the shard's knowledge of the leader for failure simulation
            with self.leader_id_lock:
                leader_to_fail_id = self._current_leader_id
            current_leader_replica = self.replicas.get(leader_to_fail_id)
            if current_leader_replica and current_leader_replica.state == "UP" and random.random() < FAILURE_PROBABILITY:
                 locked_print(f"[Shard S{self.shard_id}] *** Randomly failing leader R{leader_to_fail_id} ***")
                 current_leader_replica.simulate_failure()

    def _run_message_processing(self):
        """Background task to process message queues for all replicas."""
        while not self.stop_event.is_set():
            processed = False
            for replica in self.replicas.values():
                if replica.state == "UP" and not replica.message_queue.empty():
                    replica.process_messages(); processed = True
            if not processed: time.sleep(0.1) # Avoid busy-waiting
            if self.stop_event.is_set(): break

    def start_background_tasks(self):
        self.stop_event.clear()
        self.leader_check_thread = threading.Thread(target=self._run_leader_checks, name=f"Shard{self.shard_id}-HealthCheck", daemon=True)
        self.leader_check_thread.start()
        self.message_processor_thread = threading.Thread(target=self._run_message_processing, name=f"Shard{self.shard_id}-MsgProc", daemon=True)
        self.message_processor_thread.start()
        locked_print(f"[Shard S{self.shard_id}] Started background tasks.")

    def shutdown(self):
        locked_print(f"[Shard S{self.shard_id}] Shutting down background tasks...")
        self.stop_event.set()
        # Wait for threads to finish
        if self.leader_check_thread and self.leader_check_thread.is_alive():
             self.leader_check_thread.join(timeout=HEARTBEAT_INTERVAL + 0.5)
        if self.message_processor_thread and self.message_processor_thread.is_alive():
             self.message_processor_thread.join(timeout=0.5)
        locked_print(f"[Shard S{self.shard_id}] Background tasks stopped.")


# --- ApplicationServer Class ---
class ApplicationServer:
    _server_count = 0
    def __init__(self, zone_name, db_shard, cluster_id, redis_conn):
        self.server_id = f"S{ApplicationServer._server_count:03d}"
        ApplicationServer._server_count += 1
        self.zone_name = zone_name
        self.db_shard = db_shard # Shard Manager
        self.cluster_id = cluster_id
        self.redis_conn = redis_conn
        self.cluster_load_key = get_cluster_load_key(self.cluster_id)
        try:
            self.redis_conn.zadd(self.cluster_load_key, {self.server_id: 0}, nx=True)
        except redis.exceptions.RedisError as e:
            locked_print(f"    [App Server {self.server_id}] Redis Error initializing load: {e}")

    def handle_request(self, roll_number):
        current_load = self.get_load_from_redis()
        thread_name = threading.current_thread().name
        locked_print(f"    [App Server {self.server_id} @ {self.zone_name}] (Thread: {thread_name}) Received request for {roll_number}. Load: {current_load}")
        time.sleep(random.uniform(1.0, 2.0)) # Simulate app processing time
        result = self.db_shard.get_result(roll_number) # Call shard manager
        locked_print(f"    [App Server {self.server_id} @ {self.zone_name}] (Thread: {thread_name}) Processed {roll_number}.")
        return result

    def increment_load_in_redis(self):
        try: self.redis_conn.zincrby(self.cluster_load_key, 1, self.server_id)
        except redis.exceptions.RedisError as e: locked_print(f"Redis Error Inc {self.server_id}: {e}")
    def decrement_load_in_redis(self):
        try:
            new_load = self.redis_conn.zincrby(self.cluster_load_key, -1, self.server_id)
            # Ensure load doesn't stay negative if multiple decrements happen
            if new_load < 0: self.redis_conn.zadd(self.cluster_load_key, {self.server_id: 0})
        except redis.exceptions.RedisError as e: locked_print(f"Redis Error Dec {self.server_id}: {e}")
    def get_load_from_redis(self):
        try:
            score = self.redis_conn.zscore(self.cluster_load_key, self.server_id)
            return int(float(score)) if score is not None else 0
        except redis.exceptions.RedisError: return -1


# --- ClusterLoadBalancer Class ---
class ClusterLoadBalancer:
    _clb_count = 0
    def __init__(self, zone_name, servers_dict, redis_conn):
        self.clb_id = f"CLB{ClusterLoadBalancer._clb_count:02d}"
        ClusterLoadBalancer._clb_count += 1
        self.zone_name = zone_name
        if not servers_dict: raise ValueError("CLB must manage servers.")
        self.servers = servers_dict # {server_id: server_object}
        self.redis_conn = redis_conn
        self.cluster_load_key = get_cluster_load_key(self.clb_id)

    def select_server_id_from_redis(self):
        """Selects the server ID with the lowest score (load) from Redis."""
        try:
            # Returns list like [('S001', 2.0)] or empty list []
            results = self.redis_conn.zrange(self.cluster_load_key, 0, 0, withscores=True)
            if results:
                return results[0][0] # Return just the server ID
            else:
                # Fallback if the sorted set is empty or query failed
                # locked_print(f"    [CLB {self.clb_id}] Warning: Redis key empty/query failed. Falling back.")
                return random.choice(list(self.servers.keys())) if self.servers else None
        except redis.exceptions.RedisError as e:
            locked_print(f"    [CLB {self.clb_id}] Redis Error selecting server: {e}")
            return None

    def forward_request(self, roll_number):
        thread_name = threading.current_thread().name
        target_server_id = self.select_server_id_from_redis()
        if target_server_id and target_server_id in self.servers:
            target_server = self.servers[target_server_id]
            target_server.increment_load_in_redis() # Increment load before handling
            result = "Error: Processing Failed by Server" # Default error
            try:
                # locked_print(f"  [CLB {self.clb_id}] (Thread: {thread_name}) Forwarding {roll_number} to Server {target_server.server_id}")
                result = target_server.handle_request(roll_number)
            except Exception as e:
                locked_print(f"  [CLB {self.clb_id}] EXCEPTION during handle_request for {roll_number} on {target_server.server_id}: {e}")
                # Set a specific error message
                result = f"Error: Server {target_server.server_id} failed processing"
            finally:
                # Ensure load is decremented even if handle_request failed
                target_server.decrement_load_in_redis()
                # locked_print(f"  [CLB {self.clb_id}] (Thread: {thread_name}) Completed {roll_number} by Server {target_server.server_id}.")
            return result
        else:
            locked_print(f"  [CLB {self.clb_id}] No server available for {roll_number}")
            return "Error: Service Unavailable (CLB)"

    def get_total_load_from_redis(self):
        """Calculates the total load across all managed servers from Redis."""
        try:
            all_loads = self.redis_conn.zrange(self.cluster_load_key, 0, -1, withscores=True)
            return sum(float(score) for _, score in all_loads)
        except redis.exceptions.RedisError:
             return float('inf') # Make this CLB unlikely to be chosen on error


# --- ZoneLoadBalancer Class ---
class ZoneLoadBalancer:
     def __init__(self, zone_id, zone_name, clbs, redis_conn):
        self.zone_id = zone_id; self.zone_name = zone_name; self.data_center = DATA_CENTERS[zone_name]
        if not clbs: raise ValueError("ZLB must manage CLBs.")
        self.clbs = clbs; self.redis_conn = redis_conn # Needed if CLBs need it
        locked_print(f" [ZLB {self.zone_name} @ {self.data_center}] Initialized, managing {len(self.clbs)} CLBs.")

     def select_clb(self):
        """Selects the CLB with the lowest total load (fetched from Redis by CLB)."""
        min_load = float('inf'); chosen_clb = None; loads = {}
        clb_candidates = self.clbs[:] # Create a copy to shuffle if needed
        random.shuffle(clb_candidates) # Add randomness for tie-breaking

        for clb in clb_candidates:
             load = clb.get_total_load_from_redis()
             loads[clb.clb_id] = load # Store for potential logging
             if load < min_load:
                 min_load = load
                 chosen_clb = clb
             # If loads are equal, the shuffle handles the random choice implicitly by order

        # if chosen_clb:
        #      locked_print(f"  [ZLB {self.zone_name}] CLB Loads (from Redis): {loads}. Selecting {chosen_clb.clb_id} (Total Load: {min_load})")
        # else:
        #      locked_print(f"  [ZLB {self.zone_name}] Could not select a CLB.")
        return chosen_clb

     def forward_request(self, roll_number):
        thread_name = threading.current_thread().name
        # locked_print(f" [ZLB {self.zone_name}] (Thread: {thread_name}) Received {roll_number}. Finding best CLB...")
        target_clb = self.select_clb()
        if target_clb:
            # locked_print(f" [ZLB {self.zone_name}] (Thread: {thread_name}) Forwarding {roll_number} to {target_clb.clb_id}")
            return target_clb.forward_request(roll_number)
        else:
            locked_print(f" [ZLB {self.zone_name}] No CLB available for {roll_number}!")
            return "Error: Zone Unavailable (ZLB)"


# --- PrimaryLoadBalancer Class ---
class PrimaryLoadBalancer:
    def __init__(self, zl_balancers):
        self.zl_balancers = zl_balancers # Dict {zone_name: ZLB object}
        locked_print(f"[PLB] Initialized. Managing Zones: {list(self.zl_balancers.keys())}")

    def get_zone_for_request(self, roll_number):
        """Determines target zone based on last digit of roll number."""
        try:
             # Ensure roll_number is treated as string
             last_digit = str(roll_number)[-1]
             zone_index = int(last_digit) % NUM_ZONES
             return ZONE_NAMES.get(zone_index, ZONE_NAMES[0]) # Default to zone 0
        except (ValueError, IndexError):
             return ZONE_NAMES[0] # Default on error

    def handle_request(self, roll_num):
        """Receives request, determines zone, forwards to appropriate ZLB."""
        thread_name = threading.current_thread().name
        # locked_print(f"\n[PLB] (Thread: {thread_name}) Received request for {roll_num}")
        target_zone = self.get_zone_for_request(roll_num)
        # locked_print(f"[PLB] (Thread: {thread_name}) Determined Zone: {target_zone}")
        if target_zone in self.zl_balancers:
            zlb = self.zl_balancers[target_zone]
            # locked_print(f"[PLB] (Thread: {thread_name}) Forwarding {roll_num} to ZLB {target_zone}")
            return zlb.forward_request(roll_num)
        else:
            locked_print(f"[PLB] Error: No ZLB found for zone {target_zone}")
            return "Error: Invalid Zone (PLB)"


# --- System Setup Function ---
def setup_system(redis_conn_arg):
    """Initializes all components of the distributed system simulation."""
    # Use the argument, not the global redis_client directly during setup
    if not redis_conn_arg:
         locked_print("FATAL ERROR in setup_system: redis_conn_arg is None!")
         # Decide how to handle this - maybe raise an exception?
         raise ValueError("Redis connection is required for setup")

    locked_print("\n--- System Setup (with Bully Algorithm) ---")
    # Clear Redis keys
    locked_print("Clearing existing cluster load keys in Redis...")
    prefix = get_cluster_load_key("CLB*").split('*')[0]
    try:
        keys_to_delete = redis_conn_arg.keys(f"{prefix}*") # Use passed arg
        if keys_to_delete:
             deleted = redis_conn_arg.delete(*keys_to_delete) # Use passed arg
             locked_print(f"  Deleted {deleted} Redis keys matching {prefix}*")
        else:
             locked_print(f"  No keys found matching {prefix}*")
    except Exception as e:
        locked_print(f"ERROR clearing Redis keys: {e}")
        raise # Re-raise the exception as this is critical

    # Reset global counters for reproducible IDs if setup is called multiple times
    ApplicationServer._server_count = 0
    ClusterLoadBalancer._clb_count = 0

    # 1. Create Database Shards (each managing its replicas)
    # Ensure the DatabaseShard init doesn't need redis_conn_arg directly
    local_db_shards = [DatabaseShard(shard_id=i) for i in range(NUM_ZONES)]
    locked_print(f"Created {len(local_db_shards)} Database Shards (each with {NUM_REPLICAS_PER_SHARD} replicas).")

    # 2. Create Application Servers
    all_servers = []; servers_by_zone = {zn: [] for zn in ZONE_NAMES.values()}; servers_for_clb = {}
    temp_clb_counter = 0
    for zone_id in range(NUM_ZONES):
        zone_name = ZONE_NAMES[zone_id]; db_shard_manager = local_db_shards[zone_id]
        for clb_idx_in_zone in range(CLUSTERS_PER_ZONE):
             clb_id = f"CLB{temp_clb_counter:02d}"; servers_for_clb[clb_id] = {}
             for _ in range(SERVERS_PER_CLUSTER):
                 # Pass the passed redis_conn_arg
                 server = ApplicationServer(zone_name, db_shard_manager, clb_id, redis_conn_arg)
                 all_servers.append(server); servers_by_zone[zone_name].append(server)
                 servers_for_clb[clb_id][server.server_id] = server
             temp_clb_counter += 1
    locked_print(f"Created {len(all_servers)} Application Servers.")

    # 3. Create CLBs
    clbs_by_zone = {zn: [] for zn in ZONE_NAMES.values()}; current_clb_ids = sorted(servers_for_clb.keys()); clb_id_idx = 0
    for zone_id in range(NUM_ZONES):
        zone_name = ZONE_NAMES[zone_id]
        for _ in range(CLUSTERS_PER_ZONE):
            if clb_id_idx < len(current_clb_ids):
                clb_id = current_clb_ids[clb_id_idx]; cluster_servers_dict = servers_for_clb[clb_id]
                # Pass the passed redis_conn_arg
                clb = ClusterLoadBalancer(zone_name, cluster_servers_dict, redis_conn_arg)
                if clb.clb_id != clb_id: raise SystemError(f"FATAL ERROR: CLB ID mismatch!")
                clbs_by_zone[zone_name].append(clb); clb_id_idx += 1
            else: break
    locked_print(f"Created {sum(len(clbs) for clbs in clbs_by_zone.values())} CLBs.")

    # 4. Create ZLBs
    zl_balancers = {}
    for zone_id, zone_name in ZONE_NAMES.items():
        zone_clbs = clbs_by_zone.get(zone_name, [])
        if zone_clbs:
            # Pass the passed redis_conn_arg
            zl_balancers[zone_name] = ZoneLoadBalancer(zone_id, zone_name, zone_clbs, redis_conn_arg)
    locked_print(f"Created {len(zl_balancers)} ZLBs.")

    # 5. Create PLB
    plb = PrimaryLoadBalancer(zl_balancers=zl_balancers)
    locked_print("--- System Setup Complete ---")
    return plb, local_db_shards # Return the created instances


# --- API Endpoints ---
@app.route('/api/status', methods=['GET'])
def get_system_status():
    # API handlers should use the globally initialized clients/objects
    global redis_client, database_shards
    if not redis_client: return jsonify({"error": "Redis client not available (API)"}), 503
    if not database_shards: return jsonify({"error": "Database shards not available (API)"}), 503

    zone_status_data = []
    total_active_connections = 0
    for zone_id, zone_name in ZONE_NAMES.items():
        zone_total_load = 0; zone_total_servers = 0
        start_clb_index = zone_id * CLUSTERS_PER_ZONE; end_clb_index = start_clb_index + CLUSTERS_PER_ZONE
        zone_clb_ids = [f"CLB{i:02d}" for i in range(start_clb_index, end_clb_index)]
        for clb_id in zone_clb_ids:
            cluster_key = get_cluster_load_key(clb_id)
            try:
                server_loads = redis_client.zrange(cluster_key, 0, -1, withscores=True)
                if server_loads:
                    zone_total_servers += len(server_loads)
                    cluster_total_load = sum(float(score) for _, score in server_loads)
                    zone_total_load += cluster_total_load
            except Exception as e: locked_print(f"[API Status] Error processing {cluster_key}: {e}")
        MAX_LOAD_PER_SERVER = 5; theoretical_max_zone_load = zone_total_servers * MAX_LOAD_PER_SERVER
        average_load_percent = min(100,(zone_total_load/theoretical_max_zone_load)*100) if theoretical_max_zone_load > 0 else 0
        status = 'high-load' if average_load_percent > 85 else ('medium-load' if average_load_percent > 60 else 'healthy')
        replica_count = NUM_REPLICAS_PER_SHARD; leader_id = "N/A"; down_replicas = 0
        try: # Protect against errors during shard access in API
            if database_shards and len(database_shards) > zone_id:
                 shard = database_shards[zone_id]; leader = shard.get_current_leader()
                 if leader: leader_id = f"R{leader.replica_id}"
                 replica_count = len(shard.replicas); down_replicas = sum(1 for r in shard.replicas.values() if r.state == "DOWN")
        except Exception as e:
            locked_print(f"[API Status] Error accessing shard {zone_id} info: {e}")

        zone_data = {"name": zone_name, "status": status, "load": round(average_load_percent, 1), "dataCenterId": DATA_CENTERS.get(zone_name, "Unknown"), "clusterCount": CLUSTERS_PER_ZONE, "serversPerCluster": SERVERS_PER_CLUSTER, "replicaCount": replica_count, "currentLeader": leader_id, "downReplicas": down_replicas}
        zone_status_data.append(zone_data); total_active_connections += zone_total_load
    simulated_active_users = int(total_active_connections * 100 + random.randint(5000, 15000))
    total_avg_load = sum(z['load'] for z in zone_status_data) / len(zone_status_data) if zone_status_data else 0
    base_response_time = 150; load_factor = max(0, total_avg_load - 40)
    simulated_response_time = int(base_response_time + load_factor * 3 + random.randint(-20, 20))
    total_down_replicas = sum(z.get('downReplicas', 0) for z in zone_status_data)
    simulated_incidents = total_down_replicas
    status_response = {"zones": zone_status_data, "activeUsers": simulated_active_users, "responseTime": simulated_response_time, "incidents": simulated_incidents}
    return jsonify(status_response), 200


@app.route('/api/result/<string:roll_number>', methods=['GET'])
def get_student_result(roll_number):
    # API handlers use the globally initialized objects
    global primary_load_balancer
    if not primary_load_balancer: return jsonify({"error": "System not initialized (API)"}), 503
    api_thread_name = threading.current_thread().name
    locked_print(f"[API] (Thread: {api_thread_name}) Received request for {roll_number}")
    try:
        result = primary_load_balancer.handle_request(roll_number)
        if result and isinstance(result, str) and "Error:" in result:
             locked_print(f"[API] (Thread: {api_thread_name}) Error processing {roll_number}: {result}")
             status_code = 500;
             if "Unavailable" in result: status_code = 503
             if "Invalid Zone" in result: status_code = 400
             return jsonify({"rollNumber": roll_number, "error": result}), status_code
        elif result == "Result Not Found":
            locked_print(f"[API] (Thread: {api_thread_name}) Result not found for {roll_number}")
            return jsonify({"rollNumber": roll_number, "result": result}), 404
        else:
            locked_print(f"[API] (Thread: {api_thread_name}) Successfully processed {roll_number}")
            return jsonify({"rollNumber": roll_number, "result": result}), 200
    except Exception as e:
        locked_print(f"[API] (Thread: {api_thread_name}) EXCEPTION processing {roll_number}:")
        traceback.print_exc(); return jsonify({"rollNumber": roll_number, "error": "Unexpected server error"}), 500


# --- Global Initialization (runs when module is loaded/reloaded) ---
try:
    # Establish Redis connection and assign to global var
    redis_client_global_init = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client_global_init.ping()
    redis_client = redis_client_global_init
    locked_print("Successfully connected to Redis (Global Init).")

    # Setup the System using the established connection and assign to globals
    plb, shards_list = setup_system(redis_client)
    primary_load_balancer = plb
    database_shards = shards_list

except redis.exceptions.ConnectionError as e:
    locked_print(f"FATAL: Error connecting to Redis during global init: {e}")
    # Ensure globals are None if init fails
    redis_client = None
    primary_load_balancer = None
    database_shards = []
except Exception as e:
     locked_print(f"FATAL: Error during global system setup: {e}")
     traceback.print_exc()
     redis_client = None
     primary_load_balancer = None
     database_shards = []


# --- Main Execution Guard ---
if __name__ == "__main__":
    # Check if global initialization succeeded
    if not redis_client or not primary_load_balancer:
        locked_print("System could not be initialized due to errors during global setup. Exiting.")
        exit()

    # Run the Flask Web Server
    locked_print("\n--- Starting Flask Development Server ---")
    locked_print("API Endpoints available at:")
    locked_print("  http://localhost:5000/api/result/<roll_number>")
    locked_print("  http://localhost:5000/api/status")
    try:
        # use_reloader=False prevents Flask from restarting the process,
        # which avoids re-running the global initialization block unexpectedly.
        # You might need to manually restart if you change code.
        app.run(host='0.0.0.0', port=5000, debug=True, threaded=True, use_reloader=False)
    except KeyboardInterrupt:
         locked_print("\nCtrl+C received...")
    except SystemExit:
         locked_print("SystemExit caught, likely during shutdown.")
    except Exception as e:
         locked_print(f"Unhandled exception during app.run: {e}")
         traceback.print_exc()
    finally:
        # Graceful Shutdown for Bully Algorithm Threads
        locked_print("--- Flask server stopped. Shutting down background tasks... ---")
        # Use the globally stored list
        if database_shards: # Check if it was initialized
            for shard in database_shards:
                 if hasattr(shard, 'shutdown') and callable(shard.shutdown):
                     shard.shutdown()
        locked_print("--- Shutdown complete. ---")