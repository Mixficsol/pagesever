#include "sentinel_service.h"
#include "log.h"
#include "config.h"
#include <iostream>
#include <cstring>
#include <net/redis_cli.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <curl/curl.h>

extern pikiwidb::PConfig g_config;
using json = nlohmann::json;
namespace pikiwidb {

static bool Slavenoone(const std::string& addr);
static bool Slaveof(const std::string& addr, const std::string& newMasterAddr);
static struct curl_slist* headers = nullptr;
CURL* curl = curl_easy_init();

SentinelService::SentinelService() = default;

SentinelService::~SentinelService() {
  Stop();
}

void SentinelService::Start() {
  pika_sentinel_addr_ = fmt::v9::format("{}:{}", g_config.ip, g_config.port);
  running_ = true;
  thread_ = std::thread(&SentinelService::Run, this);
}

void SentinelService::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}

// Initialize the CURL environment and HTTP header
void InitCurl() {
  curl_global_init(CURL_GLOBAL_ALL);
  curl = curl_easy_init();
  if (curl) {
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
  }
}

// Clear the CURL environment
void CleanupCurl() {
  if (headers) {
    curl_slist_free_all(headers);
  }
  if (curl) {
    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();
}

void parseInfoReplication(const std::string& data, InfoReplication& info) {
  size_t pos = data.find("\r\n");
  if (pos != std::string::npos) {
    std::string trimmedData = data.substr(pos + 2); // Skip past the "$221\r\n"
    std::istringstream ss(trimmedData);
    std::string line;
    while (std::getline(ss, line)) {
      if (line.find("role:") == 0) {
        info.role = line.substr(5);
      } else if (line.find("connected_slaves:") == 0) {
        info.connected_slaves = std::stoi(line.substr(17));
      } else if (line.find("slave") == 0 && info.role == "master") {
        InfoSlave slave;
        size_t ipPos = line.find("ip=");
        size_t portPos = line.find("port=");
        size_t connFdPos = line.find("conn_fd=");
        size_t lagPos = line.find("lag=");

        if (ipPos != std::string::npos && portPos != std::string::npos) {
          slave.ip = line.substr(ipPos + 3, line.find(',', ipPos) - ipPos - 3);
          slave.port = line.substr(portPos + 5, line.find(',', portPos) - portPos - 5);
        }

        if (lagPos != std::string::npos) {
          size_t dbPos = line.find("db0:", lagPos);
          if (dbPos != std::string::npos) {
            slave.offset = std::stoi(line.substr(dbPos + 4));
          }
        }

        info.slaves.push_back(slave);
      } else if (line.find("db0:binlog_offset=") == 0) {
        std::istringstream binlogStream(line.substr(18));
        binlogStream >> info.db_binlog_filenum;
        binlogStream.ignore(1, ' ');
        binlogStream >> info.db_binlog_offset;
      } else if (line.find("master_host:") == 0) {
        info.master_host = line.substr(12);
      } else if (line.find("master_port:") == 0) {
        info.master_port = line.substr(12);
      } else if (line.find("master_link_status:") == 0) {
        info.master_link_status = line.substr(19);
      }
    }
  }
}

// json Serialization function
void to_json(nlohmann::json& j, const Action& a) {
  j = nlohmann::json{{"index", a.index}, {"state", a.state}};
}

void to_json(nlohmann::json& j, const GroupServer& gs) {
  j = nlohmann::json {
          {"server", gs.addr},
          {"datacenter", gs.dataCenter},
          {"action", gs.action},
          {"role", gs.role},
          {"binlog_file_num", gs.db_binlog_filenum},
          {"binlog_offset", gs.db_binlog_offset},
          {"state", gs.state},
          {"recall_times", gs.recall_times},
          {"replica_group", gs.replica_group}
  };
}

void to_json(nlohmann::json& j, const Promoting& p) {
  j = nlohmann::json{{"index", p.index}, {"state", p.state}};
}

void to_json(nlohmann::json& j, const Group* g) {
  if (!g) {
    j = nullptr;
    return;
  }

  j = nlohmann::json {
          {"id", g->id},
          {"term_id", g->term_id},
          {"promoting", g->promoting},
          {"out_of_sync", g->out_of_sync}
  };

  j["servers"] = nlohmann::json::array();
  for (const auto& server : g->servers) {
    if (server) {
      j["servers"].push_back(*server);
    } else {
      j["servers"].push_back(nullptr);
    }
  }
}

void to_json(nlohmann::json& j, const GroupInfo& g) {
  j = json::object();
  j = nlohmann::json{
          {"group_id", g.group_id},
          {"term_id", g.term_id},
          {"master_addr", g.master_addr},
          {"slaves_addr", g.slaves_addr},
          {"pika_sentinel_addr", g.pika_sentinel_addr}
  };
}

// json deserialization function
void from_json(const json& j, Action& a) {
  if (j.contains("index")) {
    j.at("index").get_to(a.index);
  }
  if (j.contains("state")) {
    j.at("state").get_to(a.state);
  }
}

void from_json(const json& j, GroupServer& gs) {
  j.at("server").get_to(gs.addr);
  j.at("datacenter").get_to(gs.dataCenter);
  if (j.contains("action") && !j.at("action").is_null()) {
      j.at("action").get_to(gs.action);
  }
  j.at("role").get_to(gs.role);
  j.at("binlog_file_num").get_to(gs.db_binlog_filenum);
  j.at("binlog_offset").get_to(gs.db_binlog_offset);
  j.at("state").get_to(gs.state);
  j.at("recall_times").get_to(gs.recall_times);
  j.at("replica_group").get_to(gs.replica_group);
}

void from_json(const json& j, Promoting& p) {
  if (j.contains("index")) {
    j.at("index").get_to(p.index);
  }
  if (j.contains("state")) {
    j.at("state").get_to(p.state);
  }
}

void from_json(const json& j, Group& g) {
  j.at("id").get_to(g.id);
  j.at("term_id").get_to(g.term_id);
  if (j.contains("servers") && !j.at("servers").is_null()) {
    for (const auto& item : j.at("servers")) {
      auto* gs = new GroupServer;
      item.get_to(*gs);
      g.servers.push_back(gs);
    }
  }
  if (j.contains("promoting") && !j.at("promoting").is_null()) {
    j.at("promoting").get_to(g.promoting);
  }
  j.at("out_of_sync").get_to(g.out_of_sync);
}

void from_json(const nlohmann::json& j, GroupInfo& g) {
  j.at("group_id").get_to(g.group_id);
  j.at("term_id").get_to(g.term_id);
  j.at("master_addr").get_to(g.master_addr);
  j.at("slaves_addr").get_to(g.slaves_addr);
  j.at("pika_sentinel_addr").get_to(g.pika_sentinel_addr);
}

// The ip address is extracted based on the addr address
static std::string DeCodeIp(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
  if (pos != std::string::npos) {
    return serveraddr.substr(0, pos);
  }
  return serveraddr;
}

// The port is extracted based on the addr address
static int DeCodePort(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
  if (pos != std::string::npos) {
    std::string portStr = serveraddr.substr(pos + 1);
    try {
      int port = std::stoi(portStr);
      return port;
    } catch (const std::invalid_argument& e) {
      WARN("Invalid port number: {}", portStr);
    } catch (const std::out_of_range& e) {
      WARN("Port number out of range: {}", portStr);
    }
  }
  return -1;
}

// The HTTP GET callback function is used to process the response data
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

void SentinelService::DelGroup(int index) {
  std::lock_guard<std::mutex> lock(groups_mtx_);
  auto iter = groups_.find(index);
  if (iter != groups_.end()) {
    groups_.erase(index);
  } else {
    WARN("Invalid index: {}", index);
  }
}

void SentinelService::UpdateGroup(nlohmann::json jsonData) {
  int id = jsonData.at("id").get<int>();
  std::lock_guard<std::mutex> lock(groups_mtx_);
  auto iter = groups_.find(id);
  Group* group = nullptr;
  if (iter != groups_.end()) {
    group = iter->second;
    for (auto server : group->servers) {
      delete server;
    }
    group->servers.clear();
  } else {
    group = new Group();
    group->id = id;
    groups_[id] = group;
  }
  group->out_of_sync = jsonData.at("out_of_sync").get<bool>();
  group->term_id = jsonData.at("term_id").get<int>();
  group->promoting = jsonData.at("promoting").get<Promoting>();
  // 更新 servers 信息
  for (const auto& server_json : jsonData.at("servers")) {
    auto server = new GroupServer();
    server_json.get_to(*server);
    group->servers.push_back(server);
  }
}

// HTTP client
void SentinelService::HTTPClient() {
  std::string readBuffer;
  std::lock_guard<std::mutex> lock(groups_mtx_);
  if (curl) {
    std::string url = g_config.codis_dashboard_addr +  "/topom/load-meta-data";
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      WARN("curl_easy_perform() failed: {}", curl_easy_strerror(res));
    }
    // Parses the JSON data obtained from the dashboard and fills the meta information into groups_
    try {
      json jsonData = json::parse(readBuffer);
      for (const auto& item : jsonData) {
        auto* g = new Group;
        item.get_to(*g);
        auto gid = g->id;
        groups_[gid] = g;
      }
    } catch (json::parse_error& e) {
      WARN("JSON parse error: {}", e.what());
    } catch (json::type_error& e) {
      WARN("JSON type error: {}", e.what());
    }
  }
}

static bool IsGroupMaster(ReplicationState* state, Group* group) {
  return state->index == 0 && group->servers[0]->addr == state->addr;
}

Group* SentinelService::GetGroup(int gid) {
  auto it = groups_.find(gid);
  if (it != groups_.end()) {
    return it->second;
  }
  return nullptr;
}

/*
 * Determine the state status value and update the server node information
 */
void SentinelService::CheckAndUpdateGroupServerState(GroupServer* server, ReplicationState* state, Group* group) {
  // If err is true, it indicates no survival
  if (state->err) {
    INFO("subject offline addr: {}", server->addr);
    if (server->state == static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      // Node subjective offline
      server->state = static_cast<int8_t>(GroupState::GroupServerStateSubjectiveOffline);
    } else {
      // Probe failure count
      server->recall_times++;
      // If you add up to 10 times and you don't survive
      if (server->recall_times >= 10) {
        // The node is offline and the meta information is updated
        INFO("offline addr: {}", server->addr);
        server->state = static_cast<int8_t>(GroupState::GroupServerStateOffline);
        server->action.state = ActionState::Nothing;
        server->replica_group = false;
      }
      // If the node is offline and the node is a master node, it is placed in the master_offline_groups queue
      if (server->state == static_cast<int8_t>(GroupState::GroupServerStateOffline) && IsGroupMaster(state, group)) {
        master_offline_groups_.emplace_back(group);
      } else {
        // Otherwise put in the slave_offline_groups queue
        slave_offline_groups_.emplace_back(group);
      }
    }
  } else {
    // if the node is offline before, but the node is in the alive state this time, the node is online again and is added to the recover_groups queue
    if (server->state == static_cast<int8_t>(GroupState::GroupServerStateOffline)) {
      recovered_groups_.emplace_back(state);
    } else {
      // If yes, reset the meta information of the server node
      server->state = static_cast<int8_t>(GroupState::GroupServerStateNormal);
      server->recall_times = 0;
      server->replica_group = true;
      server->role = state->replication.role;
      server->db_binlog_filenum = state->replication.db_binlog_filenum;
      server->db_binlog_offset = state->replication.db_binlog_offset;
      server->action.state = ActionState::Synced;
    }
  }
}

// Send an HTTP Post to the dashboard requesting changes to the etcd meta information
static void HTTPUpdateGroup(Group* group) {
  nlohmann::json json_group = group;
  if (!curl) {
    return;
  }
  if (curl) {
    std::string response_string;
    std::string json_data = json_group.dump(4);
    std::string url = g_config.codis_dashboard_addr + "/topom/upload-meta-data";
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    CURLcode res = curl_easy_perform(curl);
    INFO("HTTP Update Post RES: {}", response_string);
    if (res != CURLE_OK) {
      WARN("curl_easy_perform() failed: {}", curl_easy_strerror(res));
    }
  }
}

void SentinelService::UpdateSlaveOfflineGroups() {
  for (auto& group : slave_offline_groups_) {
    // Update the out_of_sync value in the group, send an HTTP Post request to the dashboard, and change the etcd information
    group->out_of_sync = true;
    HTTPUpdateGroup(group);
  }
}

static void SelectNewMaster(Group* group, std::string& newMasterAddr, int& newMasterIndex) {
  GroupServer* newMasterServer = nullptr; // New master node
  // Use filnume and offset to determine which node has the latest data and select a new primary node
  for (int index = 0; index < group->servers.size(); ++index) {
    // If index is 0 and the node is still offline, it indicates that the current node is the old master node that is offline, and we can skip it
    if (index == 0 || group->servers[index]->state != static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      continue;
    }
    if (newMasterServer == nullptr) {
      newMasterServer =  group->servers[index];
      newMasterIndex = index;
      // filenum Larger data is latest
    } else if (group->servers[index]->db_binlog_filenum > newMasterServer->db_binlog_filenum) {
      newMasterServer = group->servers[index];
      newMasterIndex = index;
      // If filenum is the same size, the larger the offset value is, the newer the data is
    } else if (group->servers[index]->db_binlog_filenum == newMasterServer->db_binlog_filenum) {
      if (group->servers[index]->db_binlog_offset > newMasterServer->db_binlog_offset) {
        newMasterServer = group->servers[index];
        newMasterIndex = index;
      }
    }
  }
  if (newMasterServer == nullptr) {
    newMasterAddr = "";
    return;
  }
  // Use newMasterAddr to access the addr of the new master node
  INFO("Select newMasterAddr: {}", newMasterAddr);
  newMasterAddr = newMasterServer->addr;
}

static bool DoSwitchGroupMaster(pikiwidb::Group *group, const std::string& newMasterAddr, const int newMasterIndex) {
  if (newMasterIndex <= 0 || newMasterAddr.empty()) {
    return true;
  }
  // Send the slaveof no one command to the new master node and change its meta information
  INFO("newMasterAddr: {}", newMasterAddr);
  if (!Slavenoone(newMasterAddr)) {
    WARN("promote server {} to new master failed", newMasterAddr);
    return false;
  }
  group->servers[newMasterIndex]->role = GroupServerRoleStrings::Master;
  group->servers[newMasterIndex]->action.state = ActionState::Synced;
  // Move the server position of the new master node to the first row in groups_ because the master node is all accessed in the first row of json
  std::swap(group->servers[0], group->servers[newMasterIndex]);
  // The term-id of group increases automatically. Procedure
  group->term_id++;
  // Send an HTTP Post to the dashboard requesting changes to the etcd meta information
  HTTPUpdateGroup(group);
  // Send slaveof commands to the remaining slave nodes to change to the new master node
  for (auto& server : group->servers) {
    if (server->state != static_cast<int8_t>(GroupState::GroupServerStateNormal) || server->addr == newMasterAddr) {
      continue;
    }
    // If Slaveof fails, the state changes to SyncedFailed
    if (!Slaveof(server->addr, newMasterAddr)) {
      WARN("group {} update server {} replication relationship failed, new master: {}", group->id, newMasterIndex, newMasterAddr);
      server->action.state = ActionState::SyncedFailed;
      server->state = static_cast<int8_t>(GroupState::GroupServerStateOffline);
    } else {
      INFO("{} slaveof {} success!", server->addr, newMasterAddr);
      server->action.state = ActionState::Synced;
      server->role = GroupServerRoleStrings::Slave;
    }
  }
  return true;
}

static bool TrySwitchGroupMaster(Group* group) {
  std::string newMasterAddr;
  int newMasterIndex = -1;
  // Select a new master node
  SelectNewMaster(group, newMasterAddr, newMasterIndex);
  if (newMasterAddr.empty()) {
    WARN("group {} don't has any slaves to switch master", group->id);
    return false;
  }
  // Switch to the new master node
  return DoSwitchGroupMaster(group, newMasterAddr, newMasterIndex);
}

void SentinelService::TrySwitchGroupsToNewMaster() {
  for (auto& group : master_offline_groups_) {
    group->out_of_sync = true;
    // Change the out_of_sync information of the group, send HTTP Post requests to the dashboard, and change the etcd meta information
    HTTPUpdateGroup(group);
    INFO("Have to select new addr");
    if (!TrySwitchGroupMaster(group)) {
      WARN("group-{} switch master failed");
    }
    group->out_of_sync = false;
    HTTPUpdateGroup(group);
  }
}

std::string JoinHostPost(const std::string& master_host, const std::string& master_port) {
  return master_host + ":" + master_port;
}

static std::string GetMasterAddr(const std::string& master_host, const std::string& master_port) {
  if (master_host.empty()) {
    return "";
  }
  return JoinHostPost(master_host, master_port);
}

static bool TryFixReplicationRelationship(Group *group, GroupServer *server,
                                                    ReplicationState *state, const size_t master_offline_groups) {
  std::string curMasterAddr = group->servers[0]->addr;
  if (IsGroupMaster(state, group)) {
    // If the current node is a master node and there is a value in the offline set of the master node, no processing is required
    if (state->replication.role == GroupServerRoleStrings::Master) {
      if (master_offline_groups > 0) {
        return true;
      }
    }
    // If the offline node was previously the master node and has been offline for a long time, you need to slaveof the new master node
    if (!Slavenoone(state->addr)) {
      return false;
    }
  } else {
    // If the offline node is a slave node before the offline node, and no new master-slave relationship is generated during the offline period, the status remains the same as the original
    if (GetMasterAddr(state->replication.master_host, state->replication.master_port) == curMasterAddr) {
      return true;
    }
    // If a new master/slave relationship is created during the offline period, then slaveof the new master node needs to be re-created
    if (!Slaveof(server->addr, curMasterAddr)) {
      return false;
    }
  }
  // Reset the meta information of the server node
  server->state = static_cast<int8_t>(GroupState::GroupServerStateNormal);
  server->recall_times = 0;
  server->replica_group = true;
  server->role = GroupServerRoleStrings::Slave;
  server->db_binlog_filenum = state->replication.db_binlog_filenum;
  server->db_binlog_offset = state->replication.db_binlog_offset;
  server->action.state = ActionState::Synced;
  // Send an HTTP Post to the dashboard requesting changes to the etcd meta information
  HTTPUpdateGroup(group);
  return true;
}

void SentinelService::TryFixReplicationRelationships(size_t masterOfflineGroups) {
  for (auto& state : recovered_groups_) {
    auto group = GetGroup(state->group_id);
    if (group == nullptr) {
      WARN("group-[ {} ] is not found", state->group_id);
    }
    group->out_of_sync = true;
    // Change the out_of_sync information of the group, send HTTP Post requests to the dashboard, and change the etcd meta information
    HTTPUpdateGroup(group);
    // Since the offline node may have a new master/slave relationship change during offline i, this part is handled here
    if (!TryFixReplicationRelationship(group, state->server, state, masterOfflineGroups)) {
      WARN("group-[ {} ] fix server [ {} ] replication relationship failed", group->id, state->addr);
    } else {
      group->out_of_sync = false;
      HTTPUpdateGroup(group);
    }
  }
}

void SentinelService::RefreshMastersAndSlavesClientWithPKPing() {
  if (groups_.empty()) {
    WARN("There's no groups");
    return;
  }
  std::map<int, int> groups_info;
  // Example Establish the mapping between gid and term-id
  for (auto& group : groups_) {
    groups_info[group.second->id] = group.second->term_id;
  }
  // Since the group_info sent to nodes in the same Group must be the same, it is stored in map
  std::map<int, GroupInfo> groups_parameter;
  // Assemble the GroupInfo of the PkPing command
  for (auto& group : groups_) {
    GroupInfo group_info;
    group_info.group_id = group.second->id;
    group_info.term_id = groups_info[group.second->id];
    group_info.pika_sentinel_addr = pika_sentinel_addr_;
    for (auto &server: group.second->servers) {
      if (server->role == GroupServerRoleStrings::Master && server->state == static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
        group_info.master_addr = server->addr;
      }
      if (server->role == GroupServerRoleStrings::Slave) {
        group_info.slaves_addr.push_back(server->addr);
      }
    }
    groups_parameter[group.second->id] = group_info;
  }
  for (auto& group : groups_) {
    nlohmann::json json_groupInfo = groups_parameter[group.second->id];
    for (int index = 0; index < group.second->servers.size(); ++index) {
      auto state = new ReplicationState();
      state->addr = group.second->servers[index]->addr;
      state->server = group.second->servers[index];
      state->group_id = group.second->id;
      state->err = false;
      state->index = index;
      // Send the PkPing command to the target node
      PKPingRedis(group.second->servers[index]->addr, json_groupInfo, state);
    }
  }
}

void SentinelService::CheckMastersAndSlavesState() {
  // Send the PkPing command to probe
  recovered_groups_.clear();
  master_offline_groups_.clear();
  slave_offline_groups_.clear();
  states_.clear();
  RefreshMastersAndSlavesClientWithPKPing();
  // The status value of each node is traversed to check whether it is alive
  for (auto& state : states_) {
    auto group = GetGroup(state->group_id);
    if (group == nullptr) {
      WARN("group-[ {} ] is not found", state->group_id);
    }
    CheckAndUpdateGroupServerState(state->server, state, group);
  }
  if (!slave_offline_groups_.empty()) {
    // The slave node that is offline is processed
    UpdateSlaveOfflineGroups();
  }
  if (!master_offline_groups_.empty()) {
    // The primary node that is offline is processed
    TrySwitchGroupsToNewMaster();
  }
  if (!recovered_groups_.empty()) {
    // This section describes how to process the nodes that have been offline and come online again
    TryFixReplicationRelationships(master_offline_groups_.size());
  }
}

/*
 * Pika Sentinel Thread start
 */
void SentinelService::Run() {
  // Start HTTP-Client
  InitCurl();
  HTTPClient();
  running_ = true;
  while (running_) {
    // Check the primary/secondary status every 10 seconds
    CheckMastersAndSlavesState();
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
  CleanupCurl();
}

// PKPing Command
void SentinelService::PKPingRedis(const std::string& addr, const nlohmann::json& jsondata, ReplicationState* state) {
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    WARN("Socket creation error");
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
    WARN("Invalid address/ Address not supported");
    close(sock);
  }

  if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    WARN("Connection Failed");
    close(sock);
  }
  std::string cmd;
  std::string group_info = jsondata.dump();
  net::RedisCmdArgsType argv;
  argv.emplace_back("pkping");
  argv.emplace_back(group_info);
  INFO("Group-info: {}", group_info);
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[1024];
  ssize_t reply_length = read(sock, reply, 1024);
  if (reply_length < 0) {
    WARN("Read reply failed");
    close(sock);
    state->err = true;
    states_.emplace_back(state);
    return;
  }
  std::string reply_str(reply, reply_length);
  close(sock);
  if (reply_str.find("Replication") != std::string::npos) {
    state->err = false;
  } else {
    state->err = true;
  }
  parseInfoReplication(reply_str, state->replication);
  state->replication.role.pop_back();
  state->replication.master_link_status.pop_back();
  states_.emplace_back(state);
}

// slaveof command
static bool Slaveof(const std::string& addr, const std::string& newMasterAddr) {
  auto master_ip = DeCodeIp(newMasterAddr);
  auto master_port = DeCodePort(newMasterAddr);
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    WARN("Socket creation error");
    return false;
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
    WARN("Invalid address/ Address not supported");
    close(sock);
    return false;
  }

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    WARN("Connection Failed");
    close(sock);
    return false;
  }
  std::string cmd;
  net::RedisCmdArgsType argv;
  argv.emplace_back("Slaveof");
  argv.emplace_back(master_ip);
  argv.emplace_back(std::to_string(master_port));
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[1024];
  ssize_t reply_length = read(sock, reply, 1024);
  std::string reply_str(reply, reply_length);
  close(sock);
  INFO("{}:{} Slaveof reply", host, port);
  bool success = false;
  success = reply_str.find("+OK") != std::string::npos;
  return success;
}

// slaveof no one command
static bool Slavenoone(const std::string& addr) {
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    WARN("Socket creation error");
    return false;
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
    WARN("Invalid address/ Address not supported");
    close(sock);
    return false;
  }

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    WARN("Connection Failed");
    close(sock);
    return false;
  }
  std::string cmd;
  net::RedisCmdArgsType argv;
  argv.emplace_back("Slaveof");
  argv.emplace_back("no");
  argv.emplace_back("one");
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);
  char reply[1024];
  ssize_t reply_length = read(sock, reply, 1024);
  std::string reply_str(reply, reply_length);

  close(sock);
  bool success = false;
  INFO("Slaveof noone reply {}", reply_str);
  if (reply_str.find("OK") != std::string::npos) {
    success = true;
  }
  return success;
}

} // namespace pikiwidb