/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "common.h"
#include "tcp_connection.h"

#include <set>
#include <unordered_map>
#include <unordered_set>
#include "proto_parser.h"

namespace pikiwidb {

enum ClientFlag {
  ClientFlag_multi = 0x1,
  ClientFlag_dirty = 0x1 << 1,
  ClientFlag_wrongExec = 0x1 << 2,
  ClientFlag_master = 0x1 << 3,
};

class PClient : public std::enable_shared_from_this<PClient> {
 public:
  PClient() = delete;
  explicit PClient(TcpConnection* obj);

  int HandlePackets(pikiwidb::TcpConnection*, const char*, int);

  void OnConnect();

  EventLoop* GetEventLoop(void) const { return tcp_connection_->GetEventLoop(); }
  TcpConnection* GetTcpConnection(void) const { return tcp_connection_; }

  const std::string& PeerIP() const { return tcp_connection_->GetPeerIp(); }
  int PeerPort() const { return tcp_connection_->GetPeerPort(); }

  void Close();

  static PClient* Current();

  // multi
  void SetFlag(unsigned flag) { flag_ |= flag; }
  void ClearFlag(unsigned flag) { flag_ &= ~flag; }
  bool IsFlagOn(unsigned flag) { return flag_ & flag; }
  void FlagExecWrong() {
    if (IsFlagOn(ClientFlag_multi)) {
      SetFlag(ClientFlag_wrongExec);
    }
  }

  bool Watch(int dbno, const std::string& key);
  bool NotifyDirty(int dbno, const std::string& key);
  bool Exec();
  void ClearMulti();
  void ClearWatch();

  // pubsub
  std::size_t Subscribe(const std::string& channel) { return channels_.insert(channel).second ? 1 : 0; }

  std::size_t UnSubscribe(const std::string& channel) { return channels_.erase(channel); }

  std::size_t PSubscribe(const std::string& channel) { return pattern_channels_.insert(channel).second ? 1 : 0; }

  std::size_t PUnSubscribe(const std::string& channel) { return pattern_channels_.erase(channel); }

  const std::unordered_set<std::string>& GetChannels() const { return channels_; }
  const std::unordered_set<std::string>& GetPatternChannels() const { return pattern_channels_; }
  std::size_t ChannelCount() const { return channels_.size(); }
  std::size_t PatternChannelCount() const { return pattern_channels_.size(); }
  const std::unordered_set<std::string> WaitingKeys() const { return waiting_keys_; }
  void ClearWaitingKeys() { waiting_keys_.clear(), target_.clear(); }
  const std::string& GetTarget() const { return target_; }

  void SetName(const std::string& name) { name_ = name; }
  const std::string& GetName() const { return name_; }
  static void AddCurrentToMonitor();
  static void FeedMonitors(const std::vector<std::string>& params);

  void SetAuth() { auth_ = true; }
  bool GetAuth() const { return auth_; }
  void RewriteCmd(std::vector<std::string>& params) { parser_.SetParams(params); }

 private:
  int handlePacket(pikiwidb::TcpConnection*, const char*, int);
  int handlePacketNew(pikiwidb::TcpConnection* obj, const std::vector<std::string>& params, const std::string& cmd);
  int processInlineCmd(const char*, size_t, std::vector<std::string>&);
  void reset();
  bool isPeerMaster() const;

  TcpConnection* const tcp_connection_;

  PProtoParser parser_;
  UnboundedBuffer reply_;

  std::unordered_set<std::string> channels_;
  std::unordered_set<std::string> pattern_channels_;

  unsigned flag_;
  std::unordered_map<int, std::unordered_set<std::string> > watch_keys_;
  std::vector<std::vector<std::string> > queue_cmds_;

  // blocked list
  std::unordered_set<std::string> waiting_keys_;
  std::string target_;

  std::string name_;

  // auth
  bool auth_ = false;
  time_t last_auth_ = 0;

  static thread_local PClient* s_current;
};

}  // namespace pikiwidb