// Copyright 2018, Bosch Software Innovations GmbH.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROSBAG2_TRANSPORT__ROSBAG2_TRANSPORT_HPP_
#define ROSBAG2_TRANSPORT__ROSBAG2_TRANSPORT_HPP_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rosbag2_storage/storage_options.hpp"

#include "rosbag2_transport/play_options.hpp"
#include "rosbag2_transport/record_options.hpp"
#include "rosbag2_transport/visibility_control.hpp"

namespace rosbag2_cpp
{
class Info;
class Reader;
class Writer;
class Reindexer;
}  // namespace rosbag2_cpp

namespace rosbag2_transport
{

class Rosbag2Node;

class Rosbag2Transport
{
public:
  /// Default constructor
  ROSBAG2_TRANSPORT_PUBLIC
  Rosbag2Transport();

  /// Constructor for testing, allows to set the reader and writer to use
  ROSBAG2_TRANSPORT_PUBLIC
  Rosbag2Transport(
    std::shared_ptr<rosbag2_cpp::Reader> reader,
    std::shared_ptr<rosbag2_cpp::Writer> writer,
    std::shared_ptr<rosbag2_cpp::Info> info,
    std::shared_ptr<rosbag2_cpp::Reindexer> reindexer);

  ROSBAG2_TRANSPORT_PUBLIC
  void init();

  ROSBAG2_TRANSPORT_PUBLIC
  void shutdown();

  /**
   * Records topics to a bagfile. Subscription happens at startup time, hence the topics must
   * exist when "record" is called.
   *
   * \param storage_options Options regarding the storage (e.g. bag file name)
   * \param record_options Options regarding the recording (e.g. the topics to record)
   */
  ROSBAG2_TRANSPORT_PUBLIC
  void record(
    const rosbag2_storage::StorageOptions & storage_options,
    const RecordOptions & record_options);

  /**
   * Replay all topics in a bagfile.
   *
   * \param storage_options Option regarding the storage (e.g. bag file name)
   * \param play_options Options regarding the playback (e.g. queue size)
   */
  ROSBAG2_TRANSPORT_PUBLIC
  void play(
    const rosbag2_storage::StorageOptions & storage_options,
    const PlayOptions & play_options);

  /**
   * Print the bag info contained in the metadata yaml file.
   *
   * \param uri path to the metadata yaml file.
   */
  ROSBAG2_TRANSPORT_PUBLIC
  void print_bag_info(const std::string & uri, const std::string & storage_id);

  /**
   * Reconstruct the metadata yaml file from the stored data in the bag.
   *
   * \param uri path to the bag
   * \param storage_options Options regarding the storage (e.g. bag file name)
   * \param record_options Options regarding how the file was recorded (e.g. compression format)
   */
  ROSBAG2_TRANSPORT_PUBLIC
  void reindex(const StorageOptions & storage_options);

private:
  std::shared_ptr<Rosbag2Node> setup_node(
    std::string node_prefix = "",
    const std::vector<std::string> & topic_remapping_options = {});

  std::shared_ptr<rosbag2_cpp::Reader> reader_;
  std::shared_ptr<rosbag2_cpp::Writer> writer_;
  std::shared_ptr<rosbag2_cpp::Info> info_;
  std::shared_ptr<rosbag2_cpp::Reindexer> reindexer_;

  std::shared_ptr<Rosbag2Node> transport_node_;
};

}  // namespace rosbag2_transport

#endif  // ROSBAG2_TRANSPORT__ROSBAG2_TRANSPORT_HPP_
